from pymodbus.client import ModbusTcpClient
import struct
import time

HOST = "192.168.8.1"
PORT = 200
SLAVE_ID = 3

# Register 3055 (manual Schneider)
REGISTER = 3055
COUNT = 2


def regs_to_float(regs):
    # Schneider biasanya Big Endian Word Order
    raw = struct.pack(">HH", regs[0], regs[1])
    return struct.unpack(">f", raw)[0]


client = ModbusTcpClient(
    host=HOST,
    port=PORT,
)

if not client.connect():
    print("Failed to connect")
    exit(1)

print("Connected")

while True:
    try:
        result = client.read_holding_registers(
            address=REGISTER - 1,
            count=COUNT,
            slave=SLAVE_ID
        )

        if result.isError():
            print("Read error:", result)
        else:
            power_kw = regs_to_float(result.registers)

            print(
                f"Power = {power_kw:.3f} kW "
                f"(raw={result.registers})"
            )

    except Exception as e:
        print("Exception:", e)

    time.sleep(1)