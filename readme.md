
Powershell Syslog Code

```powershell
$port = 5514
$client = New-Object System.Net.Sockets.TcpClient("127.0.0.1", $port)
$stream = $client.GetStream()

$msg = "<34>Jan  1 00:00:00 myhost myapp: hello syslog over tcp`n"
$bytes = [System.Text.Encoding]::UTF8.GetBytes($msg)

$stream.Write($bytes, 0, $bytes.Length)
$stream.Flush()

$stream.Close()
$client.Close()
```


```
.\localsyslog2pubsub.exe `
  -port 45514
  -topic "projects/YOUR_PROJECT_ID/topics/my-topic" `
  -https_proxy "http://proxy.example.com:8080" `
  -google_application_credentials "C:\keys\pubsub-publisher.json"
```