Building the C++ engine stub in containers

Two container options are provided:

- `Dockerfile.linux`: builds the engine using Ubuntu + gcc and CMake. Works on Linux or Docker Desktop.
- `Dockerfile.windows`: builds the engine using Windows Server Core and Visual Studio Build Tools (requires Windows host with Windows containers enabled).

Linux build (recommended if you can use a Linux toolchain):

```bash
# from repo root
cd prototype/docker
docker build -f Dockerfile.linux -t sysmon-engine-linux ..
docker run --rm sysmon-engine-linux
```

This will build `/src/engine/build/engine_stub` inside the image. To extract the binary to the host, use a temporary container and `docker cp`.

Windows build (requires Windows host):

```powershell
cd prototype\docker
docker build -f Dockerfile.windows -t sysmon-engine-win ..
docker run --rm sysmon-engine-win
```

Notes:
- Windows container builds require considerable disk and network access to download Visual Studio Build Tools via Chocolatey.
- If you only need a quick native binary for local testing, building with MSVC locally (Developer PowerShell) is usually faster than using a Windows container.
