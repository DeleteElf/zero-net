@echo off
chcp 65001
echo 执行时间：%date% %time%
echo 当前环境变量:%PATH%

echo go编译dll，如果需要不影响qt6的调试，需要使用1.25.9的版本编译才行。1.25.0及以下已经验证不可行！！！

echo 我们使用mingw64进行go的cgo方面编译支持，注意环境需要安装gcc支持！

echo 根据现有资料，大致不行的原因是因为go内部的cgo在低版本没有能够很好地处理cdb和gdb之间调试服务的问题，会引发cdb崩溃。

echo 我们的客户端程序因为必须使用msvc进行编译，而msvc必须使用cdb进行调试，需要特别注意！！！！

echo 正在删除旧版本
del output\network-quic.dll
del output\network-quic.lib
del output\network-quic.h
del output\network-quic.exp

echo 正在创建版本信息
windres -i ./app.rc -o ./main/app.syso

echo 正在生成dll
go build -ldflags="-s -w" -buildmode=c-shared -o ./output/network-quic.dll ./main

echo 正在生成lib
set PATH=%PATH%;C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\14.44.35207\bin\Hostx64\x64
lib /def:output/network-quic.def /machine:x64 /out:output/network-quic.lib
echo 完成时间：%date% %time%

