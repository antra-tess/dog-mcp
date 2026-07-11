# Pi Bridge Deployment

The Go2 stays in AP mode (STA/wifi join is unreliable); a Raspberry Pi 5 sits on
both networks and runs the whole stack. No IP-level routing or NAT is involved —
the relay bridge only makes outbound connections, so the Pi just needs to be on
both networks at once.

```
[Go2 AP mode] ←wifi→ [Pi wlan0: 192.168.12.x]
                     [Pi: go2_server_v2.py + relay_bridge.py + web UI]
                     [Pi eth0 → home LAN → internet] → Railway relay → remote clients
```

## Network setup (once)

Join the dog's AP on wlan0, but never use it as a default route / DNS source
(the dog's DHCP offers a dead-end default route):

```bash
sudo nmcli con add type wifi ifname wlan0 con-name dog \
    ssid "Go2_XXXXX" wifi-sec.key-mgmt wpa-psk wifi-sec.psk "<ap-password>" \
    ipv4.never-default yes ipv4.ignore-auto-dns yes ipv6.method disabled \
    connection.autoconnect yes connection.autoconnect-priority 10
sudo nmcli con up dog
```

Internet stays on eth0 (or a second wifi adapter). Verify with `ip route`:
the only `default` route must point at your router, not 192.168.12.1.

## App setup

```bash
rsync -az --exclude venv --exclude __pycache__ dog_mcp/ rpi1@<pi>:~/dog_mcp/
ssh rpi1@<pi>
sudo apt-get install -y portaudio19-dev
cd ~/dog_mcp && python3 -m venv venv
./venv/bin/pip install websockets pydantic mcp ./unitree_webrtc_connect
cp deploy/env.example .env && chmod 600 .env   # fill in real values
sudo cp deploy/go2-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now go2-server go2-relay go2-webui
```

In AP mode the robot is always `192.168.12.1` (`GO2_ROBOT_IP` in `.env`).
`WebRTCConnectionMethod.LocalSTA` with an explicit IP works fine from the AP side.

## Notes

- Web UI: `http://<pi>:8000` (works over LAN or Tailscale; it connects to the
  WebSocket server on the same hostname, port 8765).
- Only one relay bridge may claim a `robot_id` at a time — the relay rejects
  duplicates with `1008 Robot already connected`. If the bridge can't connect,
  find and stop the stale bridge instance.
- Trade-off vs STA mode: the dog is the hotspot, so it must stay in wifi range
  of the Pi.
