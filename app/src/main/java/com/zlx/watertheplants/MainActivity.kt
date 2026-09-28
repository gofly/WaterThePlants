package com.zlx.watertheplants

import android.content.Context
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Scaffold
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.material3.*
import androidx.compose.material3.pulltorefresh.PullToRefreshBox
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client
import java.nio.charset.StandardCharsets


// 1. 定义水泵数据模型
data class PumpDevice(
    val id: String,
    val status: String
)

class MainActivity : ComponentActivity() {

    private val BROKER_HOST = "broker.emqx.io"
    private val BROKER_PORT = 1883
    private val CLIENT_ID = "water-the-plants-app"
    private val MQTT_USER = "emqx"
    private val MQTT_PASSWORD = "public"
    private val TOPIC_STATUS_WILDCARD = "watertheplants/+/status"

    private lateinit var mqttClient: Mqtt5AsyncClient

    private val statusUpdateHandler = Handler(Looper.getMainLooper())
    private var isMqttConnected by mutableStateOf(false)
    private var connectionStatusText by mutableStateOf("")
    private val pumpDevicesMap = mutableStateMapOf<String, PumpDevice>()

    private fun executePolling(filterCondition: (PumpDevice) -> Boolean, showRefreshing: Boolean) {
        if (isMqttConnected && pumpDevicesMap.isNotEmpty()) {
            // 拍下快照防止并发修改异常
            val snapshot = pumpDevicesMap.values.toList()
            for (device in snapshot) {
                // 动态执行传入的筛选条件
                if (filterCondition(device)) {
                    val cmdTopic = "watertheplants/${device.id}/cmd"
                    if (showRefreshing) {
                        // 先设为刷新中状态
                        pumpDevicesMap[device.id] =
                            PumpDevice(id = device.id, status = "refreshing")
                    }
                    // 发送 MQTT 查询报文
                    publishMessage(cmdTopic, "get_status", showToast = false)
                }
            }
        }
    }

    private val fastQueryRunnable = object : Runnable {
        override fun run() {
            executePolling({ device -> device.status.startsWith("watering:") }, false)
            statusUpdateHandler.postDelayed(this, 1000)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        connectionStatusText = getString(R.string.conn_connecting)
        pumpDevicesMap["waterpump1"] = PumpDevice("waterpump1", "refreshing")
        initMqttConnection()

        setContent {
            MaterialTheme {
                Surface(
                    modifier = Modifier.fillMaxSize(),
                    color = MaterialTheme.colorScheme.background
                ) {
                    val deviceList = pumpDevicesMap.values.toList()

                    MainPumpListScreen(
                        connectionStatus = connectionStatusText,
                        isConnected = isMqttConnected,
                        devices = deviceList,
                        onRefreshClick = {
                            if (isMqttConnected) {
                                triggerManualRefresh(showToast = false)
                            } else {
                                Toast.makeText(
                                    this@MainActivity,
                                    getString(R.string.toast_not_ready),
                                    Toast.LENGTH_SHORT
                                ).show()
                            }
                        },
                        onStartAction = { deviceId, duration ->
                            val cmdTopic = "watertheplants/$deviceId/cmd"
                            publishMessage(cmdTopic, "start_water:$duration", showToast = true)
                        },
                        onStopAction = { deviceId ->
                            val cmdTopic = "watertheplants/$deviceId/cmd"
                            publishMessage(cmdTopic, "stop_water", showToast = true)
                        }
                    )
                }
            }
        }
    }

    private fun initMqttConnection() {
        mqttClient = Mqtt5Client.builder()
            .identifier(CLIENT_ID)
            .serverHost(BROKER_HOST)
            .serverPort(BROKER_PORT)
            .automaticReconnectWithDefaultConfig() // 启用默认的指数退避重连
            .addConnectedListener { _ ->
                runOnUiThread {
                    isMqttConnected = true
                    connectionStatusText = getString(R.string.conn_connected)
                    // 重新订阅通配符主题，防止断线重连后订阅失效
                    subscribeToStatusWildcard()
                    startStatusPolling()
                }
            }
            .addDisconnectedListener { context ->
                runOnUiThread {
                    isMqttConnected = false
                    connectionStatusText = getString(R.string.conn_reconnecting)
                    stopStatusPolling()
                    if (pumpDevicesMap.isNotEmpty()) {
                        val currentDeviceIds = pumpDevicesMap.keys.toList()
                        for (deviceId in currentDeviceIds) {
                            pumpDevicesMap[deviceId] = PumpDevice(id = deviceId, status = "refreshing")
                        }
                    }
                }
            }
            .buildAsync()

        mqttClient.connectWith()
            .simpleAuth()
            .username(MQTT_USER)
            .password(MQTT_PASSWORD.toByteArray(StandardCharsets.UTF_8))
            .applySimpleAuth()
            .send()
            .whenComplete { _, throwable ->
                runOnUiThread {
                    if (throwable != null) {
                        isMqttConnected = false
                        connectionStatusText =
                            getString(R.string.conn_failed, throwable.localizedMessage)
                    }
                }
            }
    }

    private fun subscribeToStatusWildcard() {
        mqttClient.subscribeWith()
            .topicFilter(TOPIC_STATUS_WILDCARD)
            .callback { publish ->
                val topic = publish.topic.toString()
                val message = String(publish.payloadAsBytes, StandardCharsets.UTF_8)
                val topicParts = topic.split("/")
                if (topicParts.size >= 3) {
                    val deviceId = topicParts[1]
                    if (pumpDevicesMap.contains(deviceId)) {
                        runOnUiThread {
                            pumpDevicesMap[deviceId] = PumpDevice(id = deviceId, status = message)
                        }

                    }
                }
            }
            .send()
            .whenComplete { _, throwable ->
                if (throwable != null) {
                    runOnUiThread {
                        Toast.makeText(
                            this,
                            getString(R.string.toast_sub_failed, throwable.localizedMessage),
                            Toast.LENGTH_SHORT
                        ).show()
                    }
                }
            }
    }

    private fun triggerManualRefresh(showToast: Boolean) {
        if (pumpDevicesMap.isEmpty()) {
            if (showToast) {
                Toast.makeText(this, getString(R.string.toast_no_device), Toast.LENGTH_SHORT).show()
            }
            return
        }

        val currentDeviceIds = pumpDevicesMap.keys.toList()

        // 1. 将所有设备先设为刷新中状态
        for (deviceId in currentDeviceIds) {
            pumpDevicesMap[deviceId] = PumpDevice(id = deviceId, status = "refreshing")

            // 🔑 核心新增：为每一台设备单独开启一个 3 秒后的超时检查炸弹
            statusUpdateHandler.postDelayed({
                // 3 秒时间到！切回主线程检查当前状态
                val currentDevice = pumpDevicesMap[deviceId]
                // 如果状态依然死卡在 "refreshing"，说明下位机没有在规定时间内通过 MQTT 应答
                if (currentDevice != null && currentDevice.status == "refreshing") {
                    pumpDevicesMap[deviceId] = PumpDevice(id = deviceId, status = "offline")
                }
            }, 3000)
        }

        // 2. 遍历发送查询指令
        for (deviceId in currentDeviceIds) {
            val cmdTopic = "watertheplants/$deviceId/cmd"
            publishMessage(cmdTopic, "get_status", showToast = false)
        }

        if (showToast) {
            Toast.makeText(this, getString(R.string.toast_refresh_sent), Toast.LENGTH_SHORT).show()
        }
    }

    private fun publishMessage(topic: String, message: String, showToast: Boolean) {
        mqttClient.publishWith()
            .topic(topic)
            .payload(message.toByteArray(StandardCharsets.UTF_8))
            .send()
            .whenComplete { _, throwable ->
                if (showToast) {
                    runOnUiThread {
                        if (throwable != null) {
                            Toast.makeText(
                                this,
                                getString(R.string.toast_send_failed, throwable.localizedMessage),
                                Toast.LENGTH_SHORT
                            ).show()
                        } else {
                            if (message == "get_status") {
                                Toast.makeText(
                                    this,
                                    getString(R.string.toast_refresh_sent),
                                    Toast.LENGTH_SHORT
                                ).show()
                            } else {
                                Toast.makeText(
                                    this,
                                    getString(R.string.toast_cmd_delivered),
                                    Toast.LENGTH_SHORT
                                ).show()
                            }
                        }
                    }
                }
            }
    }

    private fun startStatusPolling() {
        stopStatusPolling() // 先清理防止重复启动
        statusUpdateHandler.post(fastQueryRunnable)
    }

    private fun stopStatusPolling() {
        statusUpdateHandler.removeCallbacks(fastQueryRunnable)
    }

    override fun onResume() {
        super.onResume()
        if (isMqttConnected) startStatusPolling()
    }

    override fun onPause() {
        super.onPause()
        stopStatusPolling()
    }

    override fun onDestroy() {
        super.onDestroy()
        stopStatusPolling()
        if (::mqttClient.isInitialized) {
            mqttClient.disconnect()
        }
    }
}

// 2. 声明式 UI 界面主容器
@OptIn(ExperimentalMaterial3Api::class) // 启用下拉刷新实验性 API
@Composable
fun MainPumpListScreen(
    connectionStatus: String,
    isConnected: Boolean,
    devices: List<PumpDevice>,
    onRefreshClick: () -> Unit,
    onStartAction: (String, Int) -> Unit,
    onStopAction: (String) -> Unit
) {
    var showDialog by remember { mutableStateOf(false) }
    var activePumpId by remember { mutableStateOf("") }
    var sliderValue by remember { mutableStateOf(60f) }

    // 判断当前是否有设备处于刷新状态
    val isRefreshing = devices.any { it.status == "refreshing" }

    // 当 App 刚打开且成功连接到 MQTT 服务器（isConnected 变为 true）时，自动触发一次刷新请求！
    LaunchedEffect(isConnected) {
        if (isConnected) {
            onRefreshClick()
        }
    }

    Scaffold(
        topBar = {
            TopAppBar(
                title = {
                    Text(
                        text = stringResource(R.string.app_name),
                        fontWeight = FontWeight.Bold,
                        fontSize = 20.sp
                    )
                },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = MaterialTheme.colorScheme.surfaceVariant
                )
            )
        }
    ) { innerPadding ->

        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(innerPadding)
                .padding(16.dp)
        ) {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .background(
                        color = if (isConnected) Color(0xFFE8F5E9) else Color(0xFFFFEBEE),
                        shape = RoundedCornerShape(8.dp)
                    )
                    .padding(horizontal = 16.dp, vertical = 12.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text(
                    text = connectionStatus,
                    color = when {
                        isConnected -> Color(0xFF16791B)
                        else -> Color(0xFFB02E2E)
                    },
                    fontWeight = FontWeight.Bold,
                    fontSize = 15.sp,
                    modifier = Modifier.weight(1f)
                )
            }

            Spacer(modifier = Modifier.height(16.dp))

            Text(
                text = stringResource(R.string.title_pump_list, devices.size),
                fontSize = 18.sp,
                fontWeight = FontWeight.Bold,
                modifier = Modifier.padding(bottom = 8.dp)
            )

            PullToRefreshBox(
                isRefreshing = isRefreshing,
                onRefresh = onRefreshClick,
                modifier = Modifier.weight(1f)
            ) {
                if (devices.isEmpty()) {
                    Box(
                        modifier = Modifier.fillMaxSize(),
                        contentAlignment = Alignment.Center
                    ) {
                        Text(text = stringResource(R.string.text_no_device), color = Color.Gray)
                    }
                } else {
                    LazyColumn(
                        modifier = Modifier.fillMaxSize(),
                        verticalArrangement = Arrangement.spacedBy(10.dp)
                    ) {
                        items(
                            items = devices,
                            key = { it.id ?: it.hashCode().toString() }
                        ) { pump ->
                            PumpItemRow(
                                pump = pump,
                                onButtonClick = { id, currentStatus ->
                                    if (currentStatus.startsWith("watering")) {
                                        onStopAction(id)
                                    } else {
                                        activePumpId = id
                                        sliderValue = 60f
                                        showDialog = true
                                    }
                                }
                            )
                        }
                    }
                }
            }
        }
    }

    if (showDialog) {
        AlertDialog(
            onDismissRequest = { showDialog = false },
            title = {
                Text(
                    text = stringResource(R.string.dialog_title),
                    fontWeight = FontWeight.Bold,
                    fontSize = 18.sp
                )
            },
            text = {
                Column(modifier = Modifier.fillMaxWidth()) {
                    Text(
                        text = stringResource(R.string.dialog_target, activePumpId),
                        color = Color.Gray,
                        fontSize = 14.sp
                    )
                    Spacer(modifier = Modifier.height(16.dp))
                    Text(
                        text = stringResource(R.string.dialog_duration, sliderValue.toInt()),
                        fontSize = 16.sp,
                        fontWeight = FontWeight.Medium
                    )
                    Spacer(modifier = Modifier.height(8.dp))
                    Slider(
                        value = sliderValue,
                        onValueChange = { sliderValue = it },
                        valueRange = 1f..600f,
                        modifier = Modifier.fillMaxWidth()
                    )
                }
            },
            confirmButton = {
                Button(
                    onClick = {
                        showDialog = false
                        onStartAction(activePumpId, sliderValue.toInt())
                    },
                    colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF16791B))
                ) {
                    Text(stringResource(R.string.btn_start_confirm), color = Color.White)
                }
            },
            dismissButton = {
                TextButton(onClick = { showDialog = false }) {
                    Text(stringResource(R.string.dialog_cancel), color = Color.Gray)
                }
            }
        )
    }
}


// 3. 列表单项 UI 组件
@Composable
fun PumpItemRow(
    pump: PumpDevice,
    onButtonClick: (String, String) -> Unit
) {
    val context = LocalContext.current
    val isWatering = pump.status.startsWith("watering")

    // 判断当前设备是否处于离线状态
    val isOffline = pump.status == "offline"

    // 判断当前设备是否处于刷新状态
    val isRefreshing = pump.status == "refreshing"

    val localStatusText = remember(pump.status) {
        parseStatusToLocalText(context, pump.status)
    }

    Card(
        modifier = Modifier.fillMaxWidth(),
        elevation = CardDefaults.cardElevation(defaultElevation = 2.dp)
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.SpaceBetween
        ) {
            Column {
                Text(
                    text = stringResource(R.string.text_device_id, pump.id),
                    fontWeight = FontWeight.Bold,
                    fontSize = 16.sp
                )
                Spacer(modifier = Modifier.height(4.dp))
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text(text = stringResource(R.string.status_label), color = Color.Gray, fontSize = 14.sp)
                    Text(
                        text = localStatusText,
                        // 状态文本颜色微调：浇水中显示绿色，离线显示淡灰色，其他显示普通灰色
                        color = when {
                            isWatering -> Color(0xFF4CAF50)
                            isOffline -> Color(0xFFE13636)
                            else -> Color(0xFF757575)
                        },
                        fontWeight = FontWeight.Bold,
                        fontSize = 14.sp
                    )
                }
            }

            // 智能判定颜色、可用状态与文案的控制按钮
            Button(
                onClick = { onButtonClick(pump.id, pump.status) },
                enabled = !isOffline && !isRefreshing,
                colors = ButtonDefaults.buttonColors(
                    containerColor = if (isWatering) Color(0xFFBD352B) else Color(0xFF3E9141),
                    disabledContainerColor = Color(0xFFE0E0E0),
                    disabledContentColor = Color(0xFF9E9E9E)
                ),
                shape = RoundedCornerShape(6.dp)
            ) {
                Text(
                    text = if (isWatering) stringResource(R.string.btn_stop_water) else stringResource(R.string.btn_start_water)
                )
            }
        }
    }
}



fun parseStatusToLocalText(context: Context, rawStatus: String): String {
    return when {
        rawStatus == "offline" -> context.getString(R.string.status_offline)
        rawStatus == "idle" -> context.getString(R.string.status_idle)
        rawStatus == "refreshing" -> context.getString(R.string.status_refreshing)
        rawStatus.startsWith("watering") -> {
            if (rawStatus.contains(":")) {
                // 如果包含冒号，提取数字（如 "watering:3" -> "3"）
                val timeRemaining = rawStatus.substringAfter(":")
                context.getString(R.string.status_watering_with_time, timeRemaining)
            } else {
                // 如果仅仅是 "watering" 字符串
                context.getString(R.string.status_watering)
            }
        }

        else -> context.getString(R.string.status_unknown, rawStatus)
    }
}
