# Termux Setup Guide for TACTICOM

This guide walks you through setting up and running the TACTICOM local intercom system on Android via Termux.

## Prerequisites

- **Termux** app (from F-Droid or Play Store)
- **Termux:API** app (from the same source as Termux)
- **Termux:Boot** app (optional, for auto-start on device reboot)

## Installation Steps

### 1. Update Termux packages
```bash
pkg update
pkg upgrade
```

### 2. Install required system packages
```bash
pkg install python openssl
```

### 3. Grant Termux permissions

Open Termux:API app and grant it notification and vibration permissions so the "Ring the Host" feature works.

### 4. Install Termux:API command-line tools
```bash
pkg install termux-api
```

### 5. Clone or download the repository
```bash
git clone https://github.com/Ibrahim-EG/radio.git
cd radio
```

### 6. Install Python dependencies
```bash
pip install -r requirements.txt
```

## Running the Server

```bash
python radio.py
```

You should see output like:
```
======================================================
  TACTICOM — LOCAL INTERCOM ONLINE
======================================================
  Host Node        https://localhost:8443
  Local IP         https://192.168.x.x:8443
  Open the Local IP link on any device on this Wi-Fi.
  'Ring the Host' rings THIS phone via Termux:API, whether
  or not anyone has the page open.
======================================================
```

## Accessing from Other Devices

1. Open a browser on any device connected to the **same Wi-Fi network** as your Termux phone.
2. Navigate to the **Local IP** URL shown in the terminal (e.g., `https://192.168.1.100:8443`).
3. Accept the self-signed certificate warning (it's safe — the cert is generated locally).
4. You're in the Lobby! Create or join a session.

## Optional: Auto-start on Device Boot

To run TACTICOM automatically when your Android device starts:

1. Install **Termux:Boot** (from F-Droid or Play Store)
2. Create the boot script directory:
   ```bash
   mkdir -p ~/.termux/boot
   ```
3. Create a startup script:
   ```bash
   cat > ~/.termux/boot/tacticom << 'EOF'
   #!/data/data/com.termux/files/usr/bin/bash
   cd /path/to/radio
   python radio.py
   EOF
   chmod +x ~/.termux/boot/tacticom
   ```
   Replace `/path/to/radio` with the actual path to your `radio` directory.
4. Grant Termux:Boot permission to run on startup.

## Troubleshooting

**"termux-api commands not found"**
- Install the **Termux:API** app (not just the package).
- Run `pkg install termux-api`.
- Grant notification + vibration permissions to Termux:API.

**"getUserMedia() only works in a secure context"**
- The server auto-generates a self-signed TLS certificate on first run.
- If `cert.pem` and `key.pem` are missing and OpenSSL fails, generate manually:
  ```bash
  openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=tacticom.local"
  ```

**Connection refused / Can't reach the server**
- Ensure your phone and client device are on the **same Wi-Fi network**.
- Check your phone's local IP: `ifconfig | grep inet` in Termux.
- Make sure Termux has network permission in Android settings.

## Performance Notes

- **Audio quality**: 48 kHz mono PCM (broadcast quality).
- **Latency**: 80ms per chunk (kept short for low latency).
- **Max message size**: 1 MB per WebSocket frame.
- **Jitter buffer**: 120ms to smooth network jitter.
- **Backlog cap**: 600ms max queued audio (drops stale packets if Wi-Fi drops mid-stream).

## Security

- **No internet required** — LAN-only, entirely local.
- **Self-signed TLS** — safe on local networks, generated on first run.
- **Optional session codes** — protect rooms with access codes.
- **Per-device profiles** — saved in browser localStorage, not sent to the server until used.
