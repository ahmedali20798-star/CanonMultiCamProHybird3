package com.canonmulticam.pro.engine

import android.app.Application
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.VibrationEffect
import android.os.Vibrator
import android.os.VibratorManager
import androidx.core.content.FileProvider
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.canonmulticam.pro.data.CameraLabelStore
import com.canonmulticam.pro.data.TakeLogger
import com.canonmulticam.pro.domain.CameraSlotState
import com.canonmulticam.pro.domain.ExposureSettings
import com.canonmulticam.pro.domain.LatestFrameSlot
import com.canonmulticam.pro.domain.StreamQuality
import com.canonmulticam.pro.gl.CubeLutData
import com.canonmulticam.pro.gl.CubeLutParser
import com.canonmulticam.pro.transport.DiscoveredCamera
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.util.concurrent.atomic.AtomicReferenceArray

@Suppress("TooManyFunctions")
class MultiCamViewModel(application: Application) : AndroidViewModel(application) {

    private val appContext = application.applicationContext
    private val labelStore = CameraLabelStore(appContext)
    val thermalGuard = ThermalGuard(appContext)
    val takeLogger = TakeLogger()

    @Volatile var frameDeliveryCallback: ((slotIndex: Int, slot: LatestFrameSlot) -> Unit)? = null
    @Volatile var slotDisconnectedCallback: ((slotIndex: Int) -> Unit)? = null

    val histogramBins = AtomicReferenceArray<FloatArray>(4)

    private val _selectedSlotIndex = MutableStateFlow(0)
    val selectedSlotIndex: StateFlow<Int> = _selectedSlotIndex.asStateFlow()

    private val _soloSlotIndex = MutableStateFlow<Int?>(null)
    val soloSlotIndex: StateFlow<Int?> = _soloSlotIndex.asStateFlow()

    private val _isGroupRecording = MutableStateFlow(false)
    val isGroupRecording: StateFlow<Boolean> = _isGroupRecording.asStateFlow()

    private val _recordingStartTimeMs = MutableStateFlow<Long?>(null)
    val recordingStartTimeMs: StateFlow<Long?> = _recordingStartTimeMs.asStateFlow()

    private val _recordFailureBanner = MutableStateFlow<String?>(null)
    val recordFailureBanner: StateFlow<String?> = _recordFailureBanner.asStateFlow()

    private val _canUndoMatch = MutableStateFlow(false)
    val canUndoMatch: StateFlow<Boolean> = _canUndoMatch.asStateFlow()

    private var preMatchSnapshot: Map<Int, ExposureSettings> = emptyMap()
    private var isAppInBackground = false
    private var exposurePollJob: Job? = null

    lateinit var actors: List<CameraSessionActor>

    init {
        actors = List(4) { idx ->
            CameraSessionActor(
                slotIndex = idx,
                labelStore = labelStore,
                thermalLevelProvider = {
                    val base = thermalGuard.thermalLevel.value
                    val connectedCount = slotsState.value.count { it.isConnected }
                    if (connectedCount >= 3 && base == ThermalLevel.NOMINAL) {
                        ThermalLevel.MODERATE
                    } else {
                        base
                    }
                },
                onUnexpectedRecordStop = { failedSlot ->
                    triggerEmergencyHaptic()
                    val header = actors.getOrNull(failedSlot)?.state?.value?.displayHeader
                        ?: "كاميرا #${failedSlot + 1}"
                    _recordFailureBanner.value = "تحذير حرج: توقف التسجيل فجأة في $header!"
                },
                onDisconnectedCallback = { disconnectedSlot ->
                    histogramBins.set(disconnectedSlot, null)
                    slotDisconnectedCallback?.invoke(disconnectedSlot)
                },
                onFrameUpdated = { slotIdx, slot ->
                    frameDeliveryCallback?.invoke(slotIdx, slot)
                }
            )
        }
    }

    val slotsState: StateFlow<List<CameraSlotState>> = combine(
        actors[0].state,
        actors[1].state,
        actors[2].state,
        actors[3].state
    ) { s0, s1, s2, s3 -> listOf(s0, s1, s2, s3) }
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.Eagerly,
            initialValue = actors.map { it.state.value }
        )

    init {
        thermalGuard.start()
        startExposureSyncLoop()
    }

    fun onCameraDiscovered(camera: DiscoveredCamera) {
        val existing = actors.firstOrNull {
            it.state.value.boundUuid == camera.uuid || it.state.value.ipAddress == camera.ipAddress
        }
        if (existing != null) {
            if (!existing.state.value.isConnected ||
                existing.state.value.ipAddress != camera.ipAddress
            ) {
                existing.connect(camera.ipAddress, computeTargetQuality(existing.slotIndex))
            }
            return
        }

        val freeActor = actors.firstOrNull {
            !it.state.value.isConnected && it.state.value.ipAddress == null
        } ?: return

        freeActor.connect(camera.ipAddress, computeTargetQuality(freeActor.slotIndex))
    }

    fun onCameraDetachedByUuid(uuid: String) {
        actors.firstOrNull { it.state.value.boundUuid == uuid }?.disconnect()
    }

    fun bindOrReconnectCamera(slotIndex: Int, ipAddress: String) {
        if (slotIndex !in 0..3) return
        actors[slotIndex].connect(ipAddress, computeTargetQuality(slotIndex))
    }

    fun selectSlot(slotIndex: Int) {
        if (slotIndex in 0..3) _selectedSlotIndex.value = slotIndex
    }

    fun toggleSoloZoom(slotIndex: Int) {
        val nextSolo = if (_soloSlotIndex.value == slotIndex) null else slotIndex
        _soloSlotIndex.value = nextSolo
        if (nextSolo != null) _selectedSlotIndex.value = nextSolo
        applyDynamicStreamQualities()
    }

    fun setAppBackgrounded(backgrounded: Boolean) {
        isAppInBackground = backgrounded
        applyDynamicStreamQualities()
    }

    private fun applyDynamicStreamQualities() {
        actors.forEach { actor ->
            actor.updateStreamQuality(computeTargetQuality(actor.slotIndex))
        }
    }

    private fun computeTargetQuality(slotIndex: Int): StreamQuality {
        if (isAppInBackground) return StreamQuality.PAUSED
        val solo = _soloSlotIndex.value ?: return StreamQuality.LOW_QUAD
        return if (solo == slotIndex) StreamQuality.FULL_SOLO else StreamQuality.PAUSED
    }

    fun toggleGroupRecord() {
        val targetRecord = !_isGroupRecording.value
        viewModelScope.launch {
            val activeActors = actors.filter { it.state.value.isConnected }
            if (activeActors.isEmpty()) return@launch

            val results = activeActors.map { actor ->
                async { actor.slotIndex to actor.setRecordingWithAck(targetRecord) }
            }.awaitAll()

            val failedSlots = results.filter { !it.second }.map { it.first }
            if (targetRecord) {
                if (failedSlots.size < activeActors.size) {
                    _isGroupRecording.value = true
                    _recordingStartTimeMs.value = System.currentTimeMillis()
                    val camNames = activeActors.map { it.state.value.displayHeader }
                    takeLogger.startTake(camNames)
                }
            } else {
                _isGroupRecording.value = false
                _recordingStartTimeMs.value = null
                takeLogger.stopCurrentTake()
            }

            if (failedSlots.isNotEmpty()) {
                triggerEmergencyHaptic()
                val failedNames = failedSlots.joinToString(", ") {
                    actors[it].state.value.displayHeader
                }
                _recordFailureBanner.value =
                    "تنبيه: فشل أمر ${if (targetRecord) "التسجيل" else "الإيقاف"} في: $failedNames"
            } else {
                _recordFailureBanner.value = null
            }
        }
    }

    fun dismissFailureBanner() {
        _recordFailureBanner.value = null
    }

    fun onSlateTriggered() {
        if (_isGroupRecording.value) {
            takeLogger.incrementSlateOnCurrentTake()
        }
    }

    fun shareTakeLogCsv() {
        val csvContent = takeLogger.exportPremiereMarkersCsv()
        runCatching {
            val logsDir = File(appContext.cacheDir, "take_logs").apply { mkdirs() }
            val outFile = File(logsDir, "CanonMultiCam_TakeLog_${System.currentTimeMillis()}.csv")
            outFile.writeText(csvContent)

            val uri = FileProvider.getUriForFile(
                appContext,
                "${appContext.packageName}.fileprovider",
                outFile
            )
            val shareIntent = Intent(Intent.ACTION_SEND).apply {
                type = "text/csv"
                putExtra(Intent.EXTRA_STREAM, uri)
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_ACTIVITY_NEW_TASK)
            }
            val chooser = Intent.createChooser(shareIntent, "مشاركة سجل اللقطات CSV").apply {
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            }
            appContext.startActivity(chooser)
        }
    }

    fun onTouchAf(slotIndex: Int, normX: Float, normY: Float) {
        if (slotIndex in 0..3) {
            actors[slotIndex].triggerTouchAf(normX, normY)
        }
    }

    fun renameCamera(slotIndex: Int, newName: String) {
        if (slotIndex in 0..3) {
            actors[slotIndex].renameCamera(newName)
        }
    }

    fun adjustSlotExposure(slotIndex: Int, newIso: Int? = null, newKelvin: Int? = null) {
        viewModelScope.launch {
            val actor = actors.getOrNull(slotIndex) ?: return@launch
            val current = actor.state.value.exposure
                ?: ExposureSettings("f4.0", "1/50", 800, 5600)
            val updated = current.copy(
                iso = (newIso ?: current.iso).coerceIn(100, 12800),
                kelvin = (newKelvin ?: current.kelvin).coerceIn(2500, 10000)
            )
            actor.syncExposureFrom(updated)
            actor.readExposure()
        }
    }

    fun importCubeLutAsync(
        slotIndex: Int,
        uri: Uri,
        onLutReady: (slotIndex: Int, lut: CubeLutData) -> Unit
    ) {
        viewModelScope.launch {
            val parsed = withContext(Dispatchers.IO) {
                appContext.contentResolver.openInputStream(uri)?.use { stream ->
                    CubeLutParser.parse(stream).getOrNull()
                }
            }
            if (parsed != null && slotIndex in 0..3) {
                actors[slotIndex].enableLutBit()
                onLutReady(slotIndex, parsed)
            }
        }
    }

    fun matchAllToMaster(masterSlotIndex: Int) {
        viewModelScope.launch {
            val masterExposure = actors[masterSlotIndex].readExposure() ?: return@launch
            preMatchSnapshot = actors.mapNotNull { actor ->
                val currentExp = actor.state.value.exposure ?: return@mapNotNull null
                actor.slotIndex to currentExp
            }.toMap()
            _canUndoMatch.value = preMatchSnapshot.isNotEmpty()

            actors.filter { it.slotIndex != masterSlotIndex && it.state.value.isConnected }
                .forEach { slave -> slave.syncExposureFrom(masterExposure) }
        }
    }

    fun undoMatchAll() {
        val snapshot = preMatchSnapshot
        if (snapshot.isEmpty()) return
        viewModelScope.launch {
            snapshot.forEach { (slotIdx, oldExposure) ->
                if (actors[slotIdx].state.value.isConnected) {
                    actors[slotIdx].syncExposureFrom(oldExposure)
                }
            }
            preMatchSnapshot = emptyMap()
            _canUndoMatch.value = false
        }
    }

    private fun startExposureSyncLoop() {
        exposurePollJob = viewModelScope.launch {
            while (isActive) {
                actors.filter { it.state.value.isConnected }.forEach { it.readExposure() }
                delay(2500L)
            }
        }
    }

    private fun triggerEmergencyHaptic() {
        runCatching {
            val vibrator = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                val mgr = appContext.getSystemService(VibratorManager::class.java)
                mgr?.defaultVibrator
            } else {
                @Suppress("DEPRECATION")
                appContext.getSystemService(Vibrator::class.java)
            }
            vibrator?.vibrate(
                VibrationEffect.createWaveform(longArrayOf(0, 250, 120, 250, 120, 400), -1)
            )
        }
    }

    override fun onCleared() {
        exposurePollJob?.cancel()
        thermalGuard.stop()
        actors.forEach { it.destroy() }
        super.onCleared()
    }
}
