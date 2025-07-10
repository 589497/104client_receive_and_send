import ctypes
import time
import struct
from pyiec104.iec104api import *

# --- 核心修改部分 ---

# 1. 全局数据结构，用于存储遥测数据。
#    这是一个列表，每个元素都是一个包含 [IOA, Value] 的子列表。
#    它会动态地存储所有接收到的数据点。
received_data = []

# 2. 修改后的函数，用于获取遥测数据数组。
def receive_data():
    """
    此函数返回一个数组，其中包含最新的遥测数据。
    这个数组是在 cbUpdate 回调函数中被实时更新的。
    :return: 包含 [IOA, Value] 对的列表。
    """
    # 返回列表的副本以保证数据一致性
    return list(received_data)

# --- 修改部分结束 ---


# 启用流量查看功能
VIEW_TRAFFIC = 1

# 打印 sIEC104DataAttributeID 和 sIEC104DataAttributeData 结构体信息
def vPrintDataInformation(psIEC104DataAttributeID , psIEC104DataAttributeData ):
    """
    打印 IEC 104 数据属性的相关信息。
    （此函数未作修改）
    """
    print(f" IP Address {psIEC104DataAttributeID.contents.ai8IPAddress}")
    print(f" Port Number {psIEC104DataAttributeID.contents.u16PortNumber}")
    print(f" Common Address {psIEC104DataAttributeID.contents.u16CommonAddress}")
    print(f" Typeid ID is {psIEC104DataAttributeID.contents.eTypeID} IOA   {psIEC104DataAttributeID.contents.u32IOA}")
    print(f" Datatype->{psIEC104DataAttributeData.contents.eDataType} Datasize->{ psIEC104DataAttributeData.contents.eDataSize}" )

    # 检查数据质量标志
    if(psIEC104DataAttributeData.contents.tQuality) != eIEC870QualityFlags.GD :
        if(psIEC104DataAttributeData.contents.tQuality & eIEC870QualityFlags.IV) == eIEC870QualityFlags.IV:
            print(" IEC_INVALID_FLAG")
        if(psIEC104DataAttributeData.contents.tQuality & eIEC870QualityFlags.NT) == eIEC870QualityFlags.NT:
             print(" IEC_NONTOPICAL_FLAG")
        if(psIEC104DataAttributeData.contents.tQuality & eIEC870QualityFlags.SB) == eIEC870QualityFlags.SB:
             print(" IEC_SUBSTITUTED_FLAG")
        if(psIEC104DataAttributeData.contents.tQuality & eIEC870QualityFlags.BL) == eIEC870QualityFlags.BL:
             print(" IEC_BLOCKED_FLAG")

    data_type = psIEC104DataAttributeData.contents.eDataType

    # 根据数据类型解析并打印数据
    if data_type in (eDataTypes.SINGLE_POINT_DATA, eDataTypes.DOUBLE_POINT_DATA, eDataTypes.UNSIGNED_BYTE_DATA):
        data = bytearray(ctypes.string_at(psIEC104DataAttributeData.contents.pvData, 1))
        u8data = struct.unpack('B', data)[0]
        print(f" Data : {u8data}")

    elif data_type == eDataTypes.SIGNED_BYTE_DATA:
        data = bytearray(ctypes.string_at(psIEC104DataAttributeData.contents.pvData, 1))
        i8data = struct.unpack('b', data)[0]
        print(f" Data : {i8data}")

    elif data_type == eDataTypes.UNSIGNED_WORD_DATA:
        data = bytearray(ctypes.string_at(psIEC104DataAttributeData.contents.pvData, 2))
        u16data = struct.unpack('H', data)[0]
        print(f" Data : {u16data}")

    elif data_type == eDataTypes.SIGNED_WORD_DATA:
        data = bytearray(ctypes.string_at(psIEC104DataAttributeData.contents.pvData, 2))
        i16data = struct.unpack('h', data)[0]
        print(f" Data : {i16data}")

    elif data_type == eDataTypes.UNSIGNED_DWORD_DATA:
        data = bytearray(ctypes.string_at(psIEC104DataAttributeData.contents.pvData, 4))
        u32data = struct.unpack('I', data)[0]
        print(f" Data : {u32data}")

    elif data_type == eDataTypes.SIGNED_DWORD_DATA:
        data = bytearray(ctypes.string_at(psIEC104DataAttributeData.contents.pvData, 4))
        i32data = struct.unpack('i', data)[0]
        print(f" Data : {i32data}")

    elif data_type == eDataTypes.FLOAT32_DATA:
        data = bytearray(ctypes.string_at(psIEC104DataAttributeData.contents.pvData, 4))
        f32data = struct.unpack('f', data)[0]
        print(f" Data : {f32data:.3f}")

    # 打印时间戳信息
    if psIEC104DataAttributeData.contents.sTimeStamp.u16Year != 0:
        print(f" Date : {psIEC104DataAttributeData.contents.sTimeStamp.u8Day:02}-{psIEC104DataAttributeData.contents.sTimeStamp.u8Month:02}-{psIEC104DataAttributeData.contents.sTimeStamp.u16Year:04}  DOW -{psIEC104DataAttributeData.contents.sTimeStamp.u8DayoftheWeek}")
        print(f" Time : {psIEC104DataAttributeData.contents.sTimeStamp.u8Hour:02}:{psIEC104DataAttributeData.contents.sTimeStamp.u8Minute:02}:{psIEC104DataAttributeData.contents.sTimeStamp.u8Seconds:02}:{psIEC104DataAttributeData.contents.sTimeStamp.u16MilliSeconds:03}")

# 更新回调函数
def cbUpdate(u16ObjectId, psIEC104DataAttributeID, psIEC104DataAttributeData, psIEC104UpdateParameters, ptErrorValue):
    """
    【已修改】更新回调函数，当有数据更新时被调用。
    新增的逻辑会将接收到的遥测数据存入全局的 received_data 列表中。
    """
    # --- 原有代码开始 (未删除) ---
    i16rErrorCode = ctypes.c_short()
    i16rErrorCode = 0
    print(" cbUpdate() called ")
    print(" Client ID : %u" % u16ObjectId)
    vPrintDataInformation(psIEC104DataAttributeID, psIEC104DataAttributeData)
    message = f" COT {psIEC104UpdateParameters.contents.eCause}"
    print(message)
    # --- 原有代码结束 ---

    # --- 新增逻辑开始 ---
    # 从接收到的数据中提取 IOA 和数据类型
    ioa = psIEC104DataAttributeID.contents.u32IOA
    data_type = psIEC104DataAttributeData.contents.eDataType

    # 只处理浮点型数据 (FLOAT32_DATA)
    if data_type == eDataTypes.FLOAT32_DATA:
        # 解析浮点数值
        data_bytes = bytearray(ctypes.string_at(psIEC104DataAttributeData.contents.pvData, 4))
        value = struct.unpack('f', data_bytes)[0]

        # 检查IOA是否已存在于列表中
        found = False
        for item in received_data:
            if item[0] == ioa:
                # 如果IOA已存在，则更新其值
                item[1] = value
                found = True
                break
        
        # 如果IOA不存在，则添加新的 [IOA, value] 对
        if not found:
            received_data.append([ioa, value])
    # --- 新增逻辑结束 ---

    return i16rErrorCode

# 客户端连接状态回调函数
def cbClientStatus(u16ObjectId, psIEC104ClientConnectionID , peSat, ptErrorValue):
    """
    客户端连接状态回调函数。
    （此函数未作修改）
    """
    i16rErrorCode = ctypes.c_short()
    i16rErrorCode = 0
    print(" cbClientStatus called -  from IEC104 client")

    print(" Server ID : %u" % u16ObjectId)
    print(" IP Address %s Port %u " % (psIEC104ClientConnectionID.contents.ai8IPAddress, psIEC104ClientConnectionID.contents.u16PortNumber))
    print(" Server CA : %u" % psIEC104ClientConnectionID.contents.u16CommonAddress)

    if peSat.contents.value == eStatus.CONNECTED:
        print(" Status - Connected")
    else:
        print(" Status - Disconnected")

    return i16rErrorCode

# 调试回调函数
def cbDebug(u16ObjectId,  psIEC104DebugData , ptErrorValue):
    """
    调试回调函数。
    （此函数未作修改）
    """
    i16rErrorCode = ctypes.c_short()
    i16rErrorCode = 0
    u16nav = 0

    print(f" {psIEC104DebugData.contents.sTimeStamp.u8Hour}:{psIEC104DebugData.contents.sTimeStamp.u8Minute}:{psIEC104DebugData.contents.sTimeStamp.u8Seconds} Server ID: {u16ObjectId}",end='')

    if (psIEC104DebugData.contents.u32DebugOptions & eDebugOptionsFlag.DEBUG_OPTION_TX) == eDebugOptionsFlag.DEBUG_OPTION_TX:
        print(f" send IP {psIEC104DebugData.contents.ai8IPAddress} Port {psIEC104DebugData.contents.u16PortNumber}", end='')
        print(" ->", end='')

        u16nav = 0
        for u16nav in range(int(psIEC104DebugData.contents.u16TxCount)):
            try:
                print(f" {psIEC104DebugData.contents.au8TxData[u16nav]:02x}", end='')
            except TypeError:
                print("TypeError: Check list of indices")

    if (psIEC104DebugData.contents.u32DebugOptions & eDebugOptionsFlag.DEBUG_OPTION_RX) == eDebugOptionsFlag.DEBUG_OPTION_RX:
        print(f" receive IP {psIEC104DebugData.contents.ai8IPAddress} Port {psIEC104DebugData.contents.u16PortNumber}", end='')
        print(" <-", end='')

        for u16nav in range(int(psIEC104DebugData.contents.u16RxCount)):
            print(f" {psIEC104DebugData.contents.au8RxData[u16nav]:02x}", end='')

    if (psIEC104DebugData.contents.u32DebugOptions & eDebugOptionsFlag.DEBUG_OPTION_ERROR) == eDebugOptionsFlag.DEBUG_OPTION_ERROR:
        print(f" Error message {psIEC104DebugData.contents.au8ErrorMessage}")
        print(f" ErrorCode {psIEC104DebugData.contents.iErrorCode}")
        print(f" ErrorValue {psIEC104DebugData.contents.tErrorvalue}")

    print("", flush=True)
    return i16rErrorCode

# 打印错误码及其描述
def errorcodestring(errorcode):
    """
    根据错误码获取错误描述信息。
    （此函数未作修改）
    """
    sIEC104ErrorCodeDes = sIEC104ErrorCode()
    sIEC104ErrorCodeDes.iErrorCode = errorcode
    iec104_lib.IEC104ErrorCodeString(sIEC104ErrorCodeDes)
    return sIEC104ErrorCodeDes.LongDes.decode("utf-8")

# 打印错误值及其描述
def errorvaluestring(errorvalue):
    """
    根据错误值获取错误描述信息。
    （此函数未作修改）
    """
    sIEC104ErrorValueDes = sIEC104ErrorValue()
    sIEC104ErrorValueDes.iErrorValue = errorvalue
    iec104_lib.IEC104ErrorValueString(sIEC104ErrorValueDes)
    return sIEC104ErrorValueDes.LongDes.decode("utf-8")

# 主程序
def main():
    """
    【已修改】主程序入口。
    负责初始化、配置和启动 IEC 104 客户端，然后在一个定时循环中
    获取并打印接收到的数据，最后安全地停止并释放客户端资源。
    """
    myClient = None  # 确保 myClient 在 finally 块中是可访问的
    try:
        # --- 原有代码开始 (大部分未修改) ---
        print(" \t\t**** IEC 60870-5-104 Protocol Client Library Test ****")

        if iec104_lib.IEC104GetLibraryVersion().decode("utf-8") != IEC104_VERSION:
            print(" Error: Version Number Mismatch")
            # ... (版本检查代码)
            exit(0)


        i16ErrorCode = ctypes.c_short()
        tErrorValue =  ctypes.c_short()
        sParameters = sIEC104Parameters()

        # 设置回调函数
        sParameters.eAppFlag          =  eApplicationFlag.APP_CLIENT
        sParameters.ptUpdateCallback  = IEC104UpdateCallback(cbUpdate)
        sParameters.ptDebugCallback   = IEC104DebugMessageCallback(cbDebug)
        sParameters.ptClientStatusCallback   = IEC104ClientStatusCallback(cbClientStatus)
        # ... (其他回调函数设为0或保持不变)
        sParameters.u16ObjectId				= 1

        myClient =  iec104_lib.IEC104Create(ctypes.byref(sParameters), ctypes.byref((i16ErrorCode)), ctypes.byref((tErrorValue)))
        if i16ErrorCode.value != 0:
            message = f"IEC104Create() failed: {i16ErrorCode.value} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message)
            exit(0)
        else:
            print("IEC104Create() success.")

        # 加载客户端配置
        sIEC104Config = sIEC104ConfigurationParameters()
        sIEC104Config.sClientSet.ai8SourceIPAddress ="0.0.0.0".encode('utf-8')
        sIEC104Config.sClientSet.benabaleUTCtime    =   False

        if 'VIEW_TRAFFIC' in globals() and VIEW_TRAFFIC == 1:
            sIEC104Config.sClientSet.sDebug.u32DebugOptions = (eDebugOptionsFlag.DEBUG_OPTION_RX | eDebugOptionsFlag.DEBUG_OPTION_TX)
        else:
            sIEC104Config.sClientSet.sDebug.u32DebugOptions = 0

        sIEC104Config.sClientSet.u16TotalNumberofConnection = 1
        arraypointer = (sClientConnectionParameters * sIEC104Config.sClientSet.u16TotalNumberofConnection)()
        sIEC104Config.sClientSet.psClientConParameters  = ctypes.cast(arraypointer, ctypes.POINTER(sClientConnectionParameters))

        # 设置连接参数
        arraypointer[0].ai8DestinationIPAddress="192.168.3.224".encode('utf-8')
        arraypointer[0].u16PortNumber             =   2404
        arraypointer[0].i16k                      =   12
        arraypointer[0].i16w                      =   8
        arraypointer[0].u8t0                      = 30
        arraypointer[0].u8t1                      = 15
        arraypointer[0].u8t2                      = 10
        arraypointer[0].u16t3                     = 20
        arraypointer[0].eState =  eConnectState.DATA_MODE
        arraypointer[0].u8TotalNumberofStations           =   1
        arraypointer[0].au16CommonAddress[0]          =   1
        
        # --- 修复：添加缺失的 eCOTsize 配置 ---
        arraypointer[0].eCOTsize = eCauseofTransmissionSize.COT_TWO_BYTE
        
        sIEC104Config.sClientSet.bAutoGenIEC104DataObjects = True

        i16ErrorCode = iec104_lib.IEC104LoadConfiguration(myClient, ctypes.byref(sIEC104Config), ctypes.byref((tErrorValue)))

        i16ErrorCode = iec104_lib.IEC104Start(myClient, ctypes.byref((tErrorValue)))

        # --- 原有代码结束 ---

        # --- 新增逻辑开始 ---
        print("\n客户端已启动。将运行60秒以接收数据...")
        print("您可以按 Ctrl+C 提前退出程序。")

        run_duration_seconds = 60
        print_interval_seconds = 5
        start_time = time.time()
        last_print_time = 0

        while time.time() - start_time < run_duration_seconds:
            # 每隔 print_interval_seconds 秒，获取并打印一次数据
            if time.time() - last_print_time > print_interval_seconds:
                # 1. 将收到的遥测数据赋值给 receive 变量
                receive = receive_data()

                # 2. 在 main 函数中打印 receive 变量的内容
                if receive: # 仅在有数据时打印
                    print("\n--- 主函数打印 'receive' 数组 ---")
                    print(receive)
                    print("-" * 40)

                last_print_time = time.time() # 重置打印计时器

            time.sleep(0.1) # 短暂休眠，避免CPU占用过高
        # --- 新增逻辑结束 ---

    except KeyboardInterrupt:
        print("\n程序被用户中断。")
    finally:
        # --- 确保客户端被正确停止和释放 ---
        if myClient:
            print("正在停止客户端...")
            tErrorValue = ctypes.c_short()
            i16ErrorCode = iec104_lib.IEC104Stop(myClient, ctypes.byref(tErrorValue))
            if i16ErrorCode != 0:
                message = f"IEC104Stop() failed: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
                print(message)
            else:
                print("客户端已成功停止。")

        print("程序退出。")

if __name__ == "__main__":
    main()
