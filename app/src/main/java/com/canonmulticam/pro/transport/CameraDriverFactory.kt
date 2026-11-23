package com.canonmulticam.pro.transport

import android.content.Context
import android.hardware.usb.UsbDevice
import com.canonmulticam.pro.domain.CameraHandshakeInfo
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap

object CameraDriverFactory {

    @Volatile
    private var appContext: Context? = null
    private val usbDevices = ConcurrentHashMap<String, UsbDevice>()

    fun initUsbRegistry(context: Context) {
        appContext = context.applicationContext
    }

    fun registerUsbDevice(device: UsbDevice): String {
        val uri = "usb://${device.deviceName}"
        usbDevices[uri] = device
        return uri
    }

    fun unregisterUsbDevice(device: UsbDevice) {
        usbDevices.remove("usb://${device.deviceName}")
    }

    suspend fun connectAndLockProtocol(
        targetAddress: String
    ): Result<Pair<ICameraDriver, CameraHandshakeInfo>> = withContext(Dispatchers.IO) {
        if (targetAddress.startsWith("usb://")) {
            val ctx = appContext
                ?: return@withContext Result.failure(IllegalStateException("Context not initialized"))
            val usbDevice = usbDevices[targetAddress]
                ?: return@withContext Result.failure(IllegalStateException("USB device detached"))
            val usbDriver = UsbCanonCameraDriver(ctx, usbDevice)
            val usbRes = usbDriver.handshake()
            if (usbRes.isSuccess) {
                val info = usbRes.getOrNull()
                if (info != null) return@withContext Result.success(usbDriver to info)
            }
            usbDriver.close()
            return@withContext Result.failure(
                usbRes.exceptionOrNull() ?: IllegalStateException("USB Handshake failed")
            )
        }

        val ccapiDriver = CcapiCameraDriver(targetAddress)
        val ccapiRes = ccapiDriver.handshake()
        if (ccapiRes.isSuccess) {
            val info = ccapiRes.getOrNull()
            if (info != null) return@withContext Result.success(ccapiDriver to info)
        }
        ccapiDriver.close()

        val browserRemoteDriver = BrowserRemoteCameraDriver(targetAddress)
        val brRes = browserRemoteDriver.handshake()
        if (brRes.isSuccess) {
            val info = brRes.getOrNull()
            if (info != null) return@withContext Result.success(browserRemoteDriver to info)
        }
        browserRemoteDriver.close()

        val ptpDriver = PtpIpCameraDriver(targetAddress)
        val ptpRes = ptpDriver.handshake()
        if (ptpRes.isSuccess) {
            val info = ptpRes.getOrNull()
            if (info != null) return@withContext Result.success(ptpDriver to info)
        }
        ptpDriver.close()

        Result.failure(
            ccapiRes.exceptionOrNull()
                ?: IllegalStateException("Camera unreachable on CCAPI, BrowserRemote & PTP/IP")
        )
    }
}
