@echo off

echo "执行时间：$(date)"

echo "当前环境变量:${PATH}"

echo "go编译dll，如果需要不影响qt6的调试，需要使用1.25.9的版本编译才行。1.25.0及以下已经验证不可行！！！"

echo 正在删除旧版本
rm -f ./output/libnetwork-quic.so
rm -f ./output/libnetwork-quic.a

git branch

git pull

echo 正在更新引用库
GOROOT=../go
GOPATH=../go

go mod tidy

echo 正在生成so
go build -buildmode=c-shared -ldflags="-s -w" -o ./output/libnetwork-quic.so ./main
echo 正在生成.a
go build -buildmode=c-archive -ldflags="-s -w" -o ./output/libnetwork-quic.a ./main
echo "完成时间：$(date)"

