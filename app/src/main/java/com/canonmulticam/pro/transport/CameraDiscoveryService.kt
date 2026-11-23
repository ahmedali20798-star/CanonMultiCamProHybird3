package com.canonmulticam.pro.transport

import android.content.Context
import android.net.wifi.WifiManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import okhttp3.Request
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress

data class DiscoveredCamera(
    val uuid: String,
    val ipAddress: String,
    val port: Int,
    val modelName: String,
    val ccapiBaseUrl: String
)

class CameraDiscoveryService(context: Context) {

    private val appContext = context.applicationContext
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var scanJob: Job? = null
    private var sweepTick = 0

    fun startDiscovery(
        connectedCountProvider: () -> Int,
        onCameraFound: (DiscoveredCamera) -> Unit
    ) {
        scanJob?.cancel()
        scanJob = scope.launch {
            while (isActive) {
                val connected = connectedCountProvider()
                if (connected < 4) {
                    runSsdpProbe(onCameraFound)
                    if (connected == 0 || sweepTick % 3 == 0) {
                        runSubnetSweep(onCameraFound)
                    }
                    sweepTick++
                }
                delay(4500L)
            }
        }
    }

    private fun runSsdpProbe(onCameraFound: (DiscoveredCamera) -> Unit) {
        val wifi = appContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
        val lock = wifi?.createMulticastLock("CanonMultiCamSSDP")?.apply {
            setReferenceCounted(true)
            acquire()
        }
        try {
            val query = buildString {
                append("M-SEARCH * HTTP/1.1\r\n")
                append("HOST: 239.255.255.250:1900\r\n")
                append("MAN: \"ssdp:discover\"\r\n")
                append("MX: 2\r\n")
                append("ST: urn:schemas-canon-com:service:ICPO-CameraControlAPIService:1\r\n\r\n")
            }.toByteArray(Charsets.UTF_8)

            DatagramSocket().use { socket ->
                socket.soTimeout = 1200
                val group = InetAddress.getByName("239.255.255.250")
                socket.send(DatagramPacket(query, query.size, group, 1900))
                collectSsdpResponses(socket, onCameraFound)
            }
        } catch (_: Exception) {
        } finally {
            runCatching { if (lock?.isHeld == true) lock.release() }
        }
    }

    private fun collectSsdpResponses(
        socket: DatagramSocket,
        onCameraFound: (DiscoveredCamera) -> Unit
    ) {
        val buf = ByteArray(2048)
        val endTime = System.currentTimeMillis() + 1200L
        var listening = true
        while (listening && System.currentTimeMillis() < endTime) {
            val packet = DatagramPacket(buf, buf.size)
            val received = runCatching { socket.receive(packet) }.getOrNull()
            if (received == null) {
                listening = false
            } else {
                val senderIp = packet.address?.hostAddress
                val msg = String(packet.data, 0, packet.length, Charsets.UTF_8)
                val isCanon = msg.contains("Canon", ignoreCase = true) ||
                    msg.contains("ccapi", ignoreCase = true)
                if (senderIp != null && isCanon) {
                    onCameraFound(
                        DiscoveredCamera(
                            uuid = "ssdp_$senderIp",
                            ipAddress = senderIp,
                            port = 8080,
                            modelName = "Canon EOS",
                            ccapiBaseUrl = "http://$senderIp:8080/ccapi"
                        )
                    )
                }
            }
        }
    }

    private suspend fun runSubnetSweep(onCameraFound: (DiscoveredCamera) -> Unit) {
        val localIp = NetworkBinder.localWifiIpFlow.value
        val prefixes = if (localIp != null) {
            listOf(localIp.substringBeforeLast('.', ""))
        } else {
            listOf("192.168.43", "192.168.137", "192.168.1")
        }
        val selfOctet = localIp?.substringAfterLast('.')?.toIntOrNull() ?: 1

        for (prefix in prefixes) {
            if (prefix.isBlank()) continue
            val candidates = (2..254).filter { it != selfOctet }
            candidates.chunked(12).forEach { batch ->
                batch.map { host ->
                    scope.async {
                        val ip = "$prefix.$host"
                        if (probeCcapiEndpoint(ip)) {
                            onCameraFound(
                                DiscoveredCamera(
                                    uuid = "subnet_$ip",
                                    ipAddress = ip,
                                    port = 8080,
                                    modelName = "Canon EOS",
                                    ccapiBaseUrl = "http://$ip:8080/ccapi"
                                )
                            )
                        }
                    }
                }.awaitAll()
            }
        }
    }

    private fun probeCcapiEndpoint(ip: String): Boolean {
        val hasCcapi = runCatching {
            val req = Request.Builder().url("http://$ip:8080/ccapi").get().build()
            NetworkBinder.discoveryHttpClient.newCall(req).execute().use { it.isSuccessful }
        }.getOrDefault(false)
        if (hasCcapi) return true

        return runCatching {
            val req = Request.Builder().url("http://$ip/api/cam/getprop?r=0").get().build()
            NetworkBinder.discoveryHttpClient.newCall(req).execute().use { it.isSuccessful }
        }.getOrDefault(false)
    }

    fun destroy() {
        scanJob?.cancel()
        scope.cancel()
    }
}
