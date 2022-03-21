import paho.mqtt.client as mqtt
import json
import time
import asyncio
import telnetlib
from queue import Queue

# DEVICE 별 패킷 정보
RS485_DEVICE = {
    "light": {
        "state":    { "id": "0E", "cmd": "81", },

        "power":    { "id": "0E", "cmd": "41", "ack": "C1", },
    },
    "thermostat": {
        "state":    { "id": "36", "cmd": "81", },

        "away":    { "id": "36", "cmd": "45", "ack": "00", },
        "target":   { "id": "36", "cmd": "44", "ack": "C4", },
    },
}

# MQTT Discovery를 위한 Preset 정보
DISCOVERY_DEVICE = {
    "ids": ["ezville_wallpad",],
    "name": "ezville_wallpad",
    "mf": "EzVille",
    "mdl": "EzVille Wallpad",
    "sw": "ktdo79/addons/ezville_wallpad",
}

# MQTT Discovery를 위한 Payload 정보
DISCOVERY_PAYLOAD = {
    "light": [ {
        "_intg": "light",
        "~": "ezville/light_{:0>2d}_{:0>2d}",
        "name": "ezville_light_{:0>2d}_{:0>2d}",
        "opt": True,
        "stat_t": "~/power/state",
        "cmd_t": "~/power/command",
    } ],
    "thermostat": [ {
        "_intg": "climate",
        "~": "ezville/thermostat_{:0>2d}_{:0>2d}",
        "name": "ezville_thermostat_{:0>2d}_{:0>2d}",
        "mode_stat_t": "~/power/state",
        "temp_stat_t": "~/setTemp/state",
        "temp_cmd_t": "~/setTemp/command",
        "curr_temp_t": "~/curTemp/state",
        "away_mode_stat_t": "~/away/state",
        "away_mode_cmd_t": "~/away/command",
        "modes": [ "off", "heat" ],
        "min_temp": "5",
        "max_temp": 40,
    } ],
}

# STATE 확인용 Dictionary
STATE_HEADER = {
    prop["state"]["id"]: (device, prop["state"]["cmd"])
    for device, prop in RS485_DEVICE.items()
    if "state" in prop
}

ACK_HEADER = {
    prop[cmd]["id"]: (device, prop[cmd]["ack"])
    for device, prop in RS485_DEVICE.items()
        for cmd, code in prop.items()
            if "ack" in code
}

# LOG 메시지
def log(string):
    date = time.strftime('%Y-%m-%d %p %I:%M:%S', time.localtime(time.time()))
    print('[{}] {}'.format(date, string))
    return

# CHECKSUM 및 ADD를 마지막 4 BYTE에 추가
def checksum(input_hex):
    try:
        input_hex = input_hex[:-4]
        
        # 문자열 bytearray로 변환
        packet = bytes.fromhex(input_hex)
        
        # checksum 생성
        checksum = 0
        for b in packet:
            checksum ^= b
        
        # add 생성
        add = (sum(packet) + checksum) & 0xFF 
        
        # checksum add 합쳐서 return
        return input_hex + format(checksum, '02X') + format(add, '02X')
    except:
        return None

share_dir = '/share'
config_dir = '/data'

HA_TOPIC = 'ezville'
STATE_TOPIC = HA_TOPIC + '/{}/{}/state'
ELFIN_TOPIC = 'ew11'
ELFIN_SEND_TOPIC = ELFIN_TOPIC + '/send'

def ezville_loop(config):
    
    # config 
    debug = config['DEBUG']
    mqtt_log = config['mqtt_log']
    elfin_log = config['elfin_log']
    
    # EW11 혹은 HA 전달 메시지 저장소
    MSG_QUEUE = Queue()
    # EW11에 보낼 Command 및 예상 Acknowledge 패킷 
    CMD_QUEUE = []
    
    # MQTT Discovery Que 및 모드 조절
    DISCOVERY_LIST = []
    DISCOVERY_MODE = True
    DISCOVERY_DURATION = 20
    
    # EW11 전달 패킷 중 처리 후 남은 짜투리 패킷 저장
    RESIDUE = ""
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            log("Connected to MQTT broker..")
            client.subscribe([(HA_TOPIC + '/#', 0), (ELFIN_TOPIC + '/recv', 0), (ELFIN_TOPIC + '/send', 1)])
        else:
            errcode = {1: 'Connection refused - incorrect protocol version',
                       2: 'Connection refused - invalid client identifier',
                       3: 'Connection refused - server unavailable',
                       4: 'Connection refused - bad username or password',
                       5: 'Connection refused - not authorised'}
            log(errcode[rc])
         
        
    def on_message(client, userdata, msg):
        MSG_QUEUE.put(msg)
    
    # MQTT message를 분류하여 처리
    async def process_message():
        # MSG_QUEUE의 message를 하나씩 pop
        stop = False
        while not stop:
            if MSG_QUEUE.empty():
                stop = True
            else:
                msg = msg_queue.get()
                topics = msg.topic.split('/')

                if topics[0] == HA_TOPIC and topics[-1] == 'command':
                    await HA_process(topics, msg.payload.decode('utf-8'))
                elif topics[0] == ELFIN_TOPIC and topics[-1] == 'recv':
                    await EW11_process(msg.payload.hex().upper())

    # HA에서 전달된 메시지 처리        
    async def HA_process(topics, value):
        device_info = topics[1].split('_')
        device = device_info[0]
        
        if mqtt_log:
            log('[LOG] HA ->> : {} -> {}'.format('/'.join(topics), value))

        if device in RS485_DEVICE:
            key = topics[1] + topics[2]
            idx = int(device_info[1])
            sid = int(device_info[2])
            cur_state = HOMESTATE.get(key)
            value = 'ON' if value == 'heat' else value.upper()
            if cur_state:
                if value == cur_state:
                    if debug:
                        log('[DEBUG] {} is already set: {}'.format(key, value))
                else:
                    if device == 'thermostat':
                        curTemp = HOMESTATE.get(topics[1] + 'curTemp')
                        setTemp = HOMESTATE.get(topics[1] + 'setTemp')
                        
                        if topics[2] == 'away':
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['cmd'] + '01010000')
                            recvcmd = ['NULL']
                            
                            if sendcmd:
                                QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                if debug:
                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                                    
                        elif topics[2] == 'setTemp':
                            value = int(float(value))
                            if value == int(setTemp):
                                if debug:
                                    log('[DEBUG] {} is already set: {}'.format(topics[1], value))
                            else:
                                setTemp = value
                                sendcmd = checksum('F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['cmd'] + '01' + "{:02X}".format(setTemp) + '0000')
                                recvcmd = ['F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['ack']]

                                if sendcmd:
                                    QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                                    if debug:
                                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))

#                    elif device == 'Fan':
#                        if topics[2] == 'power':
#                            sendcmd = DEVICE_LISTS[device][idx].get('command' + value)
#                            recvcmd = DEVICE_LISTS[device][idx].get('state' + value) if value == 'ON' else [
#                                DEVICE_LISTS[device][idx].get('state' + value)]
#                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
#                            if debug:
#                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
#                        elif topics[2] == 'speed':
#                            speed_list = ['LOW', 'MEDIUM', 'HIGH']
#                            if value in speed_list:
#                                index = speed_list.index(value)
#                                sendcmd = DEVICE_LISTS[device][idx]['CHANGE'][index]
#                                recvcmd = [DEVICE_LISTS[device][idx]['stateON'][index]]
#                                QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
#                                if debug:
#                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))

                    elif device == 'light':                         
                        pwr = '01' if value == 'ON' else '00'
                        
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '030' + str(sid) + pwr + '000000')

                        if sendcmd:
                            recvcmd = ['F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']]
                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
                            if debug:
                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
                        else:
                            if debug:
                                log('[DEBUG] There is no command for {}'.format('/'.join(topics)))
            else:
                if debug:
                    log('[DEBUG] There is no command about {}'.format('/'.join(topics)))
        else:
            if debug:
                log('[DEBUG] There is no command for {}'.format('/'.join(topics)))

    
    # EW11 전달된 메시지 처리
    async def EW11_process(raw_data):
        raw_data = RESIDUE + raw_data
        DISCOVERY = DISCOVERY_MODE
        
        k = 0
        cors = []
        msg_length = len(raw_data)
        while k < msg_length:
            # F7로 시작하는 패턴을 패킷으로 분리
            if raw_data[k:k + 2] == "F7":
                # 남은 데이터가 최소 패킷 길이를 만족하지 못하면 RESIDUE에 저장 후 종료
                if k + 10 > msg_length:
                    RESIDUE = raw_data[k:]
                    break
                else:
                    data_length = int(raw_data[k + 8:k + 10], 16)
                    packet_length = 10 + data_length * 2 + 4 
                    
                    # 남은 데이터가 예상되는 패킷 길이보다 짧으면 RESIDUE에 저장 후 종료
                    if k + packet_length > msg_length:
                        RESIDUE = raw_data[k:]
                        break
                    else:
                        packet = raw_data[k:k + packet_length]
                        
                # 분리된 패킷이 Valid한 패킷인지 Checksum 확인                
                if packet != checksum(packet):
                    k+=1
                    continue
                else:
                    # STATE 패킷인지 우선 확인
                    if packet[2:4] in STATE_HEADER and (packet[6:8] in STATE_HEADER[packet[2:4]][1] or packet[6:8] == ACK_HEADER[packet[2:4]][1]):
                        # 현재 DISCOVERY MODE인 경우 패킷 정보 기반 장치 등록 실시
                        if DISCOVERY:
                            name = STATE_HEADER[packet[2:4]][0]                            
                            if name == 'light':
                                # ROOM ID
                                rid = int(packet[5], 16)
                                # ROOM의 light 갯수 + 1
                                slc = int(packet[8:10], 16) 
                                
                                for id in range(1, slc):
                                    discovery_name = "{}_{:0>2d}_{:0>2d}".format(name, rid, id)
                                    
                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        payload = DISCOVERY_PAYLOAD[name][0].copy()
                                        payload["~"] = payload["~"].format(rid, id)
                                        payload["name"] = payload["name"].format(rid, id)
                                   
                                        await mttq_discovery(payload)                            
                                    else:
                                        onoff = 'ON' if int(packet[10 + 2 * id: 12 + 2 * id], 16) > 0 else 'OFF'
                                        await update_state(name, rid, id, onoff)
                                                                                    
                            elif name == 'thermostat':
                                # room 갯수
                                rc = int((int(packet[8:10], 16) - 5) / 2)
                                # room의 조절기 수 (현재 하나 뿐임)
                                src = 1
                                
                                for rid in range(1, rc + 1):
                                    discovery_name = "{}_{:0>2d}_{:0>2d}".format(name, rid, src)
                                    
                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        payload = DISCOVERY_PAYLOAD[name][0].copy()
                                        payload["~"] = payload["~"].format(rid, src)
                                        payload["name"] = payload["name"].format(rid, src)
                                   
                                        await mttq_discovery(payload)   
                                    else:
                                        curT = int(packet[16 + 4 * rid:18 + 4 * rid], 16)
                                        setT = int(packet[18 + 4 * rid:20 + 4 * rid], 16)
                                        onoff = 'ON' if int(packet[12:14], 16) & 0x1F >> (rc - rid) & 1 else 'OFF'
                                        await update_state(name, rid, src, onoff)
                                        await update_temperature(name, rid, src, curT, setT)           
                                                                                    
                        # DISCOVERY_MODE가 아닌 경우 상태 업데이트만 실시
                        else:
                            # 앞서 보낸 명령에 대한 Acknowledge 인 경우 CMD_QUEUE에서 해당 명령 삭제
                            for que in CMD_QUEUE:
                                if packet[0:8] in que['recvcmd']:
                                    QUEUE.remove(que)
                                    if debug:
                                        log('[DEBUG] Found matched hex: {}. Delete a queue: {}'.format(raw_data, que))
                                    break
                        
                            name = STATE_HEADER[packet[2:4]][0]
                  
                            if name == 'light':
                                # ROOM ID
                                rid = int(packet[5], 16)
                                # ROOM의 light 갯수 + 1
                                slc = int(packet[8:10], 16) 
                                
                                for id in range(1, slc):
                                    onoff = 'ON' if int(data[10 + 2 * id: 12 + 2 * id], 16) > 0 else 'OFF'
                                    await update_state(name, rid, id, onoff)
                                    
                            elif name == 'thermostat':
                                # room 갯수
                                rc = int((int(packet[8:10], 16) - 5) / 2)
                                # room의 조절기 수 (현재 하나 뿐임)
                                src = 1
                                
                                for rid in range(1, rc + 1):
                                    curT = int(packet[16 + 4 * rid:18 + 4 * rid], 16)
                                    setT = int(packet[18 + 4 * rid:20 + 4 * rid], 16)
                                    onoff = 'ON' if int(packet[12:14], 16) & 0x1F >> (rc - rid) & 1 else 'OFF'
                                    await update_state(name, rid, src, onoff)
                                    await update_temperature(name, rid, src, curT, setT)
                       
                RESIDUE = ""
                k = k + packet_length
            else:
                k+=1


    async def mqtt_discovery(payload):
        intg = payload.pop("_intg")

        # MQTT 통합구성요소에 등록되기 위한 추가 내용
        payload["device"] = DISCOVERY_DEVICE
        payload["uniq_id"] = payload["name"]

        # Discovery에 등록
        topic = "homeassistant/{}/ezville_wallpad/{}/config".format(intg, payload["name"])
        log("Add new device:  {}".format(topic))
        mqtt_client.publish(topic, json.dumps(payload))

                                                                                    
    async def update_state(device, id1, id2, onoff):
        state = 'power'
        deviceID = "{}_{:0>2d}_{:0>2d}".format(device, id1, id2)
        key = deviceID + state

        if onoff != HOMESTATE.get(key):
            HOMESTATE[key] = onoff
            topic = STATE_TOPIC.format(deviceID, state)

            mqtt_client.publish(topic, onoff.encode())
            if mqtt_log:
                log('[LOG] ->> HA : {} >> {}'.format(topic, onoff))
        else:
            if debug:
                log('[DEBUG] {} is already set: {}'.format(deviceID, onoff))
        return
                                                                                    
    async def update_temperature(device, id1, id2, curTemp, setTemp):
        deviceID = "{}_{:0>2d}_{:0>2d}".format(device, id1, id2)
        temperature = {'curTemp': "{:02X}".format(int(curTemp, 16)), 'setTemp': "{:02X}".format(int(setTemp, 16))}
        for state in temperature:
            key = deviceID + state
            val = temperature[state]
            if val != HOMESTATE.get(key):
                HOMESTATE[key] = val
                topic = STATE_TOPIC.format(deviceID, state)
                mqtt_client.publish(topic, val.encode())
                if mqtt_log:
                    log('[LOG] ->> HA : {} -> {}'.format(topic, val))
            else:
                if debug:
                    log('[DEBUG] {} is already set: {}'.format(key, val))
        return  
                                                                                    
                                                                                    
    async def send_to_elfin():                                                                                              
        while not DISCOVERY_MODE:
            try:
#                if time.time_ns() - COLLECTDATA['LastRecv'] > 10000000000:  # 10s
#                if time.time_ns() - COLLECTDATA['LastRecv'] > 100000000000: 
#                    log(str(COLLECTDATA['LastRecv']) + "  :  " + str(time.time_ns()))
#                    log('[WARNING] 10초간 신호를 받지 못했습니다. ew11 기기를 재시작합니다.')
#                    try:
#                        elfin_id = config['elfin_id']
#                        elfin_password = config['elfin_password']
#                        elfin_server = config['elfin_server']

#                        ew11 = telnetlib.Telnet(elfin_server)

#                        ew11.read_until(b"login:")
#                        ew11.write(elfin_id.encode('utf-8') + b'\n')
#                        ew11.read_until(b"password:")
#                        ew11.write(elfin_password.encode('utf-8') + b'\n')
#                        ew11.write('Restart'.encode('utf-8') + b'\n')

#                        await asyncio.sleep(10)
#                    except:
#                        log('[WARNING] 기기 재시작 오류! 기기 상태를 확인하세요.')
#                    COLLECTDATA['LastRecv'] = time.time_ns()
#                elif time.time_ns() - COLLECTDATA['LastRecv'] > 100000000:
                    if CMD_QUEUE:
                        send_data = CMD_QUEUE.pop(0)
                        if elfin_log:
                            log('[SIGNAL] 신호 전송: {}'.format(send_data))
                        mqtt_client.publish(ELFIN_SEND_TOPIC, bytes.fromhex(send_data['sendcmd']))

                        if send_data['count'] < 5:
                            send_data['count'] = send_data['count'] + 1
                            CMD_QUEUE.append(send_data)
                        else:
                            if elfin_log:
                                log('[SIGNAL] Send over 5 times. Send Failure. Delete a queue: {}'.format(send_data))
                    return
            except Exception as err:
                log('[ERROR] send_to_elfin(): {}'.format(err))
                return
                                                                                    
                                                                                    
    # MQTT 통신 시작
    mqtt_client = mqtt.Client('mqtt-ezville')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect_async(config['mqtt_server'])
    mqtt_client.loop_start()        
  
    target_time = time.time() + DISCOVERY_DURATION
    log('장치를 등록합니다...')
    log('======================================')
    
    async def main_run():
        while True:
            await asyncio.gather(
                process_message(),
                send_to_elfin()
            )
            await asyncio.sleep(0.001)
            if time.time() > target_time:
                DISCOVERY_MODE = False
                log('======================================')
                log('동작을 시작합니다...')
                                                                                    
    asyncio.run(main_run())

if __name__ == '__main__':
    with open(config_dir + '/options.json') as file:
        CONFIG = json.load(file)
    
    ezville_loop(CONFIG)