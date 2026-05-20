# network-quic
### network-quic working on quic-go ,it is go to c project!
### 本项目主要用于桥接quic-go，项目使用MIT协议
#### 项目环境需要 go 1.25.9 及以上版本的支持
1. build.bat 用于编译windows平台。
2. build.sh 用于编译linux平台。
3. 生成的结果存于output目录内，C或c++程序需要引用network-quic.h文件，并使用对应的静态或动态文件。

#### FAQ
1. 打包出现 go/pkg/tool/linux_amd64/link: running gcc failed: exit status 1
解决办法： 查找跨平台兼容性问题，答案来自：gemini
