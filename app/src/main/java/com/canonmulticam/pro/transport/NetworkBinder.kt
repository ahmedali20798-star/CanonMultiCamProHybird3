package com.canonmulticam.pro.transport

import android.content.Context
import android.net.ConnectivityManager
import android.net.LinkProperties
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import java.net.Inet4Address
import java.net.Socket
import java.util.concurrent.TimeUnit
import javax.net.SocketFactory

object NetworkBinder {

    private var connectivityManager: ConnectivityManager? = null
    @Volatile private var activeWifiNetwork: Network? = null

    private val _localWifiIpFlow = MutableStateFlow<String?>(null)
    val localWifiIpFlow: StateFlow<String?> = _localWifiIpFlow.asStateFlow()

    private val wifiSocketFactory = object : SocketFactory() {
        private fun bindToWifi(socket: Socket): Socket {
            runCatching { activeWifiNetwork?.bindSocket(socket) }
            return socket
        }
        override fun createSocket(): Socket = bindToWifi(Socket())
        override fun createSocket(host: String, port: Int): Socket =
            bindToWifi(Socket(host, port))
        override fun createSocket(
            host: String,
            port: Int,
            localHost: java.net.InetAddress,
            localPort: Int
        ): Socket = bindToWifi(Socket(host, port, localHost, localPort))
        override fun createSocket(host: java.net.InetAddress, port: Int): Socket =
            bindToWifi(Socket(host, port))
        override fun createSocket(
            address: java.net.InetAddress,
            port: Int,
            localAddress: java.net.InetAddress,
            localPort: Int
        ): Socket = bindToWifi(Socket(address, port, localAddress, localPort))
    }

    val wifiBoundHttpClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(2500, TimeUnit.MILLISECONDS)
        .readTimeout(2500, TimeUnit.MILLISECONDS)
        .writeTimeout(2500, TimeUnit.MILLISECONDS)
        .connectionPool(ConnectionPool(8, 30, TimeUnit.SECONDS))
        .socketFactory(wifiSocketFactory)
        .build()

    val discoveryHttpClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(350, TimeUnit.MILLISECONDS)
        .readTimeout(450, TimeUnit.MILLISECONDS)
        .writeTimeout(350, TimeUnit.MILLISECONDS)
        .connectionPool(ConnectionPool(4, 5, TimeUnit.SECONDS))
        .socketFactory(wifiSocketFactory)
        .build()

    fun init(context: Context) {
        val cm = context.applicationContext
            .getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        connectivityManager = cm

        val request = NetworkRequest.Builder()
            .addTransportType(NetworkCapabilities.TRANSPORT_WIFI)
            .build()

        cm.registerNetworkCallback(
            request,
            object : ConnectivityManager.NetworkCallback() {
                override fun onAvailable(network: Network) {
                    activeWifiNetwork = network
                    updateLocalIp(cm.getLinkProperties(network))
                }

                override fun onLinkPropertiesChanged(
                    network: Network,
                    linkProperties: LinkProperties
                ) {
                    activeWifiNetwork = network
                    updateLocalIp(linkProperties)
                }

                override fun onLost(network: Network) {
                    if (activeWifiNetwork == network) {
                        activeWifiNetwork = null
                        _localWifiIpFlow.value = null
                    }
                }
            }
        )
    }

    fun openBoundTcpSocket(ip: String, port: Int, timeoutMs: Int = 2000): Socket {
        val socket = Socket()
        runCatching { activeWifiNetwork?.bindSocket(socket) }
        socket.connect(java.net.InetSocketAddress(ip, port), timeoutMs)
        socket.soTimeout = timeoutMs
        return socket
    }

    private fun updateLocalIp(props: LinkProperties?) {
        val ipv4 = props?.linkAddresses
            ?.map { it.address }
            ?.filterIsInstance<Inet4Address>()
            ?.firstOrNull { !it.isLoopbackAddress }
            ?.hostAddress
        _localWifiIpFlow.value = ipv4
    }
}
