package com.canonmulticam.pro.engine

import android.os.SystemClock
import com.canonmulticam.pro.data.CameraLabelStore
import com.canonmulticam.pro.domain.AnamorphicRatio
import com.canonmulticam.pro.domain.CameraSlotState
import com.canonmulticam.pro.domain.ExposureSettings
import com.canonmulticam.pro.domain.LatestFrameSlot
import com.canonmulticam.pro.domain.RecordAckState
import com.canonmulticam.pro.domain.StreamQuality
import com.canonmulticam.pro.gl.ShaderToolFlags
import com.canonmulticam.pro.transport.CameraDriverFactory
import com.canonmulticam.pro.transport.ICameraDriver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

@Suppress("TooManyFunctions")
class CameraSessionActor(
    val slotIndex: Int,
    private val labelStore: CameraLabelStore,
    private val thermalLevelProvider: () -> ThermalLevel,
    private val onUnexpectedRecordStop: (Int) -> Unit,
    private val onDisconnectedCallback: (Int) -> Unit,
    private val onFrameUpdated: (Int, LatestFrameSlot) -> Unit
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val frameSlot = LatestFrameSlot()

    private val _state = MutableStateFlow(CameraSlotState(slotIndex = slotIndex))
    val state: StateFlow<CameraSlotState> = _state.asStateFlow()

    private var driver: ICameraDriver? = null
    private var sessionJob: Job? = null
    private var supportedApertures: List<String> = emptyList()
    private var supportedShutters: List<String> = emptyList()
    private var currentQuality: StreamQuality = StreamQuality.LOW_QUAD

    fun connect(targetAddress: String, initialQuality: StreamQuality) {
        currentQuality = initialQuality
        sessionJob?.cancel()
        sessionJob = scope.launch {
            _state.update { it.copy(ipAddress = targetAddress, isSignalStale = false) }
            val connRes = CameraDriverFactory.connectAndLockProtocol(targetAddress)
            val (drv, info) = connRes.getOrElse {
                _state.update {
                    it.copy(isConnected = false, ipAddress = null, recordState = RecordAckState.IDLE)
                }
                onDisconnectedCallback(slotIndex)
                return@launch
            }

            driver = drv
            supportedApertures = info.supportedApertures
            supportedShutters = info.supportedShutters
            val savedLabel = labelStore.getCustomLabel(info.uuid)

            _state.update {
                it.copy(
                    boundUuid = info.uuid,
                    ipAddress = targetAddress,
                    modelName = info.modelName,
                    customLabel = savedLabel,
                    protocol = info.protocol,
                    isConnected = true,
                    isSignalStale = false
                )
            }

            drv.setStreamQuality(currentQuality)
            runLiveViewAndTelemetryLoop(drv)
        }
    }

    private suspend fun runLiveViewAndTelemetryLoop(drv: ICameraDriver) {
        var consecutiveErrors = 0
        var loopTick = 0
        var lastUiTelemetryEmitMs = 0L
        while (scope.isActive && driver === drv) {
            if (currentQuality == StreamQuality.PAUSED) {
                delay(200L)
                continue
            }

            val t0 = SystemClock.elapsedRealtime()
            val frameRes = drv.pullNextLiveFrame(frameSlot)
            val rtt = SystemClock.elapsedRealtime() - t0

            if (frameRes.isSuccess) {
                consecutiveErrors = 0
                onFrameUpdated(slotIndex, frameSlot)
                val now = SystemClock.elapsedRealtime()
                if (_state.value.isSignalStale || now - lastUiTelemetryEmitMs >= 1000L) {
                    lastUiTelemetryEmitMs = now
                    _state.update { it.copy(isSignalStale = false, latencyMs = rtt) }
                }
            } else {
                consecutiveErrors++
                if (consecutiveErrors >= 12) {
                    _state.update { it.copy(isSignalStale = true) }
                }
                if (consecutiveErrors >= 40) {
                    disconnect()
                    return
                }
            }

            if (loopTick % 30 == 0) {
                pollTelemetry(drv)
            }
            loopTick++
            delay(thermalLevelProvider().frameDelayMs)
        }
    }

    private suspend fun pollTelemetry(drv: ICameraDriver) {
        drv.getBatteryStatus().onSuccess { (batLabel, low) ->
            _state.update { it.copy(batteryLabel = batLabel, isBatteryLow = low) }
        }
        drv.getRemainingRecordMinutes().onSuccess { mins ->
            _state.update { it.copy(remainingCardMinutes = mins) }
        }
        if (_state.value.recordState == RecordAckState.RECORDING) {
            drv.queryActualRecordingState().onSuccess { stillRecording ->
                if (!stillRecording) {
                    _state.update { it.copy(recordState = RecordAckState.ERROR) }
                    onUnexpectedRecordStop(slotIndex)
                }
            }
        }
    }

    suspend fun setRecordingWithAck(record: Boolean): Boolean {
        val drv = driver ?: return false
        _state.update { it.copy(recordState = RecordAckState.PENDING) }
        val res = drv.setRecording(record)
        return if (res.isSuccess) {
            _state.update {
                it.copy(
                    recordState = if (record) RecordAckState.RECORDING else RecordAckState.IDLE
                )
            }
            true
        } else {
            _state.update { it.copy(recordState = RecordAckState.ERROR) }
            false
        }
    }

    fun triggerTouchAf(normX: Float, normY: Float) {
        scope.launch { driver?.triggerTouchAf(normX, normY) }
    }

    suspend fun readExposure(): ExposureSettings? {
        val exp = driver?.getCurrentExposure()?.getOrNull() ?: return null
        _state.update { it.copy(exposure = exp) }
        return exp
    }

    suspend fun syncExposureFrom(master: ExposureSettings) {
        val drv = driver ?: return
        drv.applyExposure(master, supportedApertures, supportedShutters).onSuccess {
            _state.update { it.copy(exposure = master, isExposureDrifted = false) }
        }
    }

    fun updateStreamQuality(quality: StreamQuality) {
        currentQuality = quality
        scope.launch { driver?.setStreamQuality(quality) }
    }

    fun updateToolsMask(newMask: Int) {
        _state.update { it.copy(activeToolsMask = newMask) }
    }

    fun enableLutBit() {
        _state.update { it.copy(activeToolsMask = it.activeToolsMask or ShaderToolFlags.LUT) }
    }

    fun updateAnamorphic(ratio: AnamorphicRatio) {
        _state.update { it.copy(anamorphic = ratio) }
    }

    fun renameCamera(newName: String) {
        val uuid = _state.value.boundUuid ?: return
        labelStore.saveCustomLabel(uuid, newName)
        _state.update { it.copy(customLabel = newName.trim().ifEmpty { null }) }
    }

    fun disconnect() {
        sessionJob?.cancel()
        runCatching { driver?.close() }
        driver = null
        _state.value = CameraSlotState(slotIndex = slotIndex)
        onDisconnectedCallback(slotIndex)
    }

    fun destroy() {
        disconnect()
        scope.cancel()
    }
}
