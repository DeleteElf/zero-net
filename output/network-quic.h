#ifndef GO_CGO_EXPORT_PROLOGUE_H
#define GO_CGO_EXPORT_PROLOGUE_H

//在Go1.6.2版本之后，Go的runtime 加入了指针违规传递检测机制。该机制主要针对Go向C传递带有指向其他Go内存的地址，具体见文献检查机制。
//Go 1.25.9 支持自动将此处代码生成.h文件，因此，我们不再引用.h文件，而是将必须的内容书写在此

enum LogLevel {
    LevelFatal = 0,
    LevelError,
    LevelWarn,
    LevelInfo,
    LevelDebug,
    // LevelTrace,
    LevelMax
};
//声明消息回调
typedef void (*MessageCallback)(const char*);
typedef void (*MessageChannelCallback)(const char*,int);

typedef struct _NetworkData {
    int len;
    char *ptr;
} NetworkData;

typedef struct _ClientData {
	char* id;
	int index;
    int len;
    char* ptr;
} ClientData;

enum NetworkResult {
    Success = 0,
    Error=80000,
    ErrorContext,
    ErrorParam,
    ErrorSocket,
    ErrorBuffer,
    ErrorClose,
    Closed
};

#ifdef __cplusplus
extern "C" {
#endif

extern __declspec(dllexport) void InitLogCallback(int level, MessageCallback callback);
extern __declspec(dllexport) int InitNetwork(void);
extern __declspec(dllexport) int SetOnAcceptSocketCallback(MessageCallback callback);
extern __declspec(dllexport) int SetOnDisConnectedCallback(MessageCallback callback);
extern __declspec(dllexport) int ClientClose(void);
extern __declspec(dllexport) int ClientConnect(int channelCount, NetworkData* config);
extern __declspec(dllexport) int ClientChannelReceive(int chnIdx, NetworkData* data);
extern __declspec(dllexport) int ClientChannelSend(int chnIdx, NetworkData* data);
extern __declspec(dllexport) int ServerCreate(NetworkData* config);
extern __declspec(dllexport) int ServerClose(void);
extern __declspec(dllexport) int ServerStartListen(void);
extern __declspec(dllexport) int ServerSocketClose(char* clientId);
extern __declspec(dllexport) int ServerSocketSend(char* clientId, int chnIdx, NetworkData* data);
extern __declspec(dllexport) int ServerSocketReceive(ClientData* data);
extern __declspec(dllexport) int ServerSocketChannelReceive(char* clientId, int chnIdx, NetworkData* data);

#ifdef __cplusplus
}
#endif
