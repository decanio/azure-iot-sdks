// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/select.h>
#include <sys/time.h>
#include <errno.h>

#include <jansson.h>

#include "azure_c_shared_utility/platform.h"
#include "iothub_client.h"
#include "iothub_message.h"
#include "azure_c_shared_utility/threadapi.h"
#include "azure_c_shared_utility/crt_abstractions.h"
#include "iothubtransportamqp.h"

#ifdef MBED_BUILD_TIMESTAMP
#include "certs.h"
#endif // MBED_BUILD_TIMESTAMP

static const char* connectionString = "[device connection string]";
static int callbackCounter;

DEFINE_ENUM_STRINGS(IOTHUB_CLIENT_CONFIRMATION_RESULT, IOTHUB_CLIENT_CONFIRMATION_RESULT_VALUES);

typedef struct EVENT_INSTANCE_TAG
{
    IOTHUB_MESSAGE_HANDLE messageHandle;
    int messageTrackingId;  // For tracking the messages within the user callback.
} EVENT_INSTANCE;

static IOTHUBMESSAGE_DISPOSITION_RESULT ReceiveMessageCallback(IOTHUB_MESSAGE_HANDLE message, void* userContextCallback)
{
    int* counter = (int*)userContextCallback;
    const unsigned char* buffer = NULL;
    size_t size = 0;
    
    IOTHUBMESSAGE_CONTENT_TYPE contentType = IoTHubMessage_GetContentType(message);

    if (contentType == IOTHUBMESSAGE_BYTEARRAY)
    {
        if (IoTHubMessage_GetByteArray(message, &buffer, &size) == IOTHUB_MESSAGE_OK)
        {
            (void)printf("Received Message [%d] with BINARY Data: <<<%.*s>>> & Size=%d\r\n", *counter, (int)size, buffer, (int)size);
        }
        else
        {
            (void)printf("Failed getting the BINARY body of the message received.\r\n");
        }
    }
    else if (contentType == IOTHUBMESSAGE_STRING)
    {
        if ((buffer = IoTHubMessage_GetString(message)) != NULL && (size = strlen(buffer)) > 0)
        {
            (void)printf("Received Message [%d] with STRING Data: <<<%.*s>>> & Size=%d\r\n", *counter, (int)size, buffer, (int)size);
        }
        else
        {
            (void)printf("Failed getting the STRING body of the message received.\r\n");
        }
    }
    else
    {
        (void)printf("Failed getting the body of the message received (type %i).\r\n", contentType);
    }

    // Retrieve properties from the message
    MAP_HANDLE mapProperties = IoTHubMessage_Properties(message);
    if (mapProperties != NULL)
    {
        const char*const* keys;
        const char*const* values;
        size_t propertyCount = 0;
        if (Map_GetInternals(mapProperties, &keys, &values, &propertyCount) == MAP_OK)
        {
            if (propertyCount > 0)
            {
                size_t index;

                printf("Message Properties:\r\n");
                for (index = 0; index < propertyCount; index++)
                {
                    printf("\tKey: %s Value: %s\r\n", keys[index], values[index]);
                }
                printf("\r\n");
            }
        }
    }

    /* Some device specific action code goes here... */
    (*counter)++;
    return IOTHUBMESSAGE_ACCEPTED;
}

static void SendConfirmationCallback(IOTHUB_CLIENT_CONFIRMATION_RESULT result, void* userContextCallback)
{
    EVENT_INSTANCE* eventInstance = (EVENT_INSTANCE*)userContextCallback;
    (void)printf("Confirmation[%d] received for message tracking id = %d with result = %s\r\n", callbackCounter, eventInstance->messageTrackingId, ENUM_TO_STRING(IOTHUB_CLIENT_CONFIRMATION_RESULT, result));
    /* Some device specific action code goes here... */
    callbackCounter++;
    IoTHubMessage_Destroy(eventInstance->messageHandle);
    free(eventInstance);
}

static char msgText[1024];
static char propText[1024];
#define MESSAGE_COUNT 5

static size_t geteveline(char **lineptr, size_t *n, int fd, struct timeval *timeout, int *timedout)
{
    fd_set fds;
    size_t buf_len = 1024;
    *lineptr = malloc(buf_len);
    size_t i = 0;
    int eol = 0;
    int cr = 0;
    FD_ZERO(&fds);
    FD_SET(fd, &fds);
    while (!eol) {
        if (i >= buf_len) {
            *lineptr = realloc(*lineptr, buf_len + 1024);
            buf_len += 1024;
        }
        char *p = *lineptr + i;
        if (i == 0) {
            if (select(fd + 1, &fds, NULL, NULL, timeout) == 0) {
                *timedout = 1;
                return 0;
           }
        }
        size_t c = read(fd, p, 1);
        if (c > 0) {
            i += c;
            if (*p == '\n') {
                eol = 1;
                i -= 1;
                *p = '\0';
            }
        } else if (c == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
            }
        } else {
            /* eol on eof */
            eol = 1;
        }
    }

    return i;
}

void iothub_client_suri_amqp_run(int batch, const char *eve_file)
{
    IOTHUB_CLIENT_HANDLE iotHubClientHandle;

    EVENT_INSTANCE *message;

    srand((unsigned int)time(NULL));
    double avgWindSpeed = 10.0;

    callbackCounter = 0;
    int receiveContext = 0;

    (void)printf("Starting the IoTHub client sample AMQP...\r\n");

#if 1
    struct sockaddr_un addr;
    int fd,cl,rc;

    if ( (fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket error");
        exit(-1);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, eve_file, sizeof(addr.sun_path)-1);

    //unlink(socket_path);

#if 0
    int status = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

    if (status == -1) {
        perror("calling fcntl");
    }
#endif

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind error");
        exit(-1);
    }

    if (listen(fd, 5) == -1) {
        perror("listen error");
        exit(-1);
    }
#else
    FILE *jsonf = fopen(eve_file, "r");
    if (jsonf == NULL)
    {
        printf("Failed to open input file.\r\n");
        exit(-1);
    }
#endif

    if (platform_init() != 0)
    {
        printf("Failed to initialize the platform.\r\n");
        exit(-1);
    }
    if ((iotHubClientHandle = IoTHubClient_CreateFromConnectionString(connectionString, AMQP_Protocol)) == NULL)
    {
        (void)printf("ERROR: iotHubClientHandle is NULL!\r\n");
        goto end;
    }

#ifdef MBED_BUILD_TIMESTAMP
    // For mbed add the certificate information
    if (IoTHubClient_SetOption(iotHubClientHandle, "TrustedCerts", certificates) != IOTHUB_CLIENT_OK)
    {
        printf("failure to set option \"TrustedCerts\"\r\n");
    }
#endif // MBED_BUILD_TIMESTAMP

    /* Setting Message call back, so we can receive Commands. */
    if (IoTHubClient_SetMessageCallback(iotHubClientHandle, ReceiveMessageCallback, &receiveContext) != IOTHUB_CLIENT_OK)
    {
        (void)printf("ERROR: IoTHubClient_SetMessageCallback..........FAILED!\r\n");
        goto destroy_end;
    }

    (void)printf("IoTHubClient_SetMessageCallback...successful.\r\n");

    /* Now that we are ready to receive commands, let's send some messages */
    size_t i;
    for (;;)
    {
        int eof;
        if ( (cl = accept(fd, NULL, NULL)) == -1) {
            perror("accept error");
            //continue;
            exit(-1);
        }
        int status = fcntl(cl, F_SETFL, fcntl(cl, F_GETFL, 0) | O_NONBLOCK);

        if (status == -1) {
            perror("calling fcntl");
        }
        char *s;
        size_t n;
        s = NULL;
        n = 0;
        size_t len;
        int timedout = 0;
        struct timeval timeout;
        timeout.tv_sec = 10 /*60*/;
        timeout.tv_usec = 0;
        int count = 0;
        eof = 0;
        char *json_array = NULL;
        if (batch) {
            while (!eof) {
                if ((len = geteveline(&s, &n, cl, &timeout, &timedout)) > 0) {
#if 1
                    /* TBD: limit message size to 256KB */
                    if (++count == 1) {
                        json_array = malloc(len + 13);
                        snprintf(json_array, len + 13, "{\"eve_log\":[%s", s);
                    } else {
                        size_t total = strlen(json_array);
                        json_array = realloc(json_array, total + len + 2);
                        snprintf(&json_array[total],total + len + 1, ",%s", s);
                    }
#else
                    json_error_t error;
                    json_t *js_s = json_loads(s, JSON_DECODE_ANY, &error);
#endif
                    free(s);
                    i++;
                } else if (len == 0) {
                    if (json_array != NULL) {
                        size_t total = strlen(json_array);
                        json_array = realloc(json_array, total + 2);
                        strcpy(&json_array[total], "]}");
                        printf("%s\n", json_array);

                        message = calloc(1, sizeof(EVENT_INSTANCE));
                        if ((message->messageHandle = IoTHubMessage_CreateFromByteArray((const unsigned char*)json_array, strlen(json_array))) == NULL) {
                            (void)printf("ERROR: iotHubMessageHandle is NULL!\r\n");
                        } else {
                            message->messageTrackingId = i;

                            MAP_HANDLE propMap = IoTHubMessage_Properties(message->messageHandle);
                            sprintf_s(propText, sizeof(propText), "PropMsg_%d", i);
                            if (Map_AddOrUpdate(propMap, "PropName", propText) != MAP_OK) {
                                (void)printf("ERROR: Map_AddOrUpdate Failed!\r\n");
                            }

                            if (IoTHubClient_SendEventAsync(iotHubClientHandle,
                                                            message->messageHandle,
                                                            SendConfirmationCallback,
                                                            message) != IOTHUB_CLIENT_OK) {
                                (void)printf("ERROR: IoTHubClient_SendEventAsync..........FAILED!\r\n");
                            } else {
                                (void)printf("IoTHubClient_SendEventAsync accepted data for transmission to IoT Hub.\r\n");
                            }
                        }
                    }
                    if (!timedout) {
                        close(cl);
                        eof = 1;
                    }
                }
            }
        } else {
            /* non batching loop */
            while (!eof) {
                if ((len = geteveline(&s, &n, cl, &timeout, &timedout)) > 0) {
                    printf("read \"%s\"\n", s);
                    message = calloc(1, sizeof(EVENT_INSTANCE));
                    if ((message->messageHandle = IoTHubMessage_CreateFromByteArray((const unsigned char*)s, len)) == NULL) {
                        (void)printf("ERROR: iotHubMessageHandle is NULL!\r\n");
                    } else {
                        message->messageTrackingId = i;

                        MAP_HANDLE propMap = IoTHubMessage_Properties(message->messageHandle);
                        sprintf_s(propText, sizeof(propText), "PropMsg_%d", i);
                        if (Map_AddOrUpdate(propMap, "PropName", propText) != MAP_OK) {
                            (void)printf("ERROR: Map_AddOrUpdate Failed!\r\n");
                        }

                        if (IoTHubClient_SendEventAsync(iotHubClientHandle,
                                                        message->messageHandle,
                                                        SendConfirmationCallback,
                                                        message) != IOTHUB_CLIENT_OK) {
                            (void)printf("ERROR: IoTHubClient_SendEventAsync..........FAILED!\r\n");
                        } else {
                            (void)printf("IoTHubClient_SendEventAsync accepted data for transmission to IoT Hub.\r\n");
                        }
                    }
                    usleep(100);
                } else if (len == 0) {
                    if (!timedout) {
                        close(cl);
                        eof = 1;
                    }
                }
            }
        }
    }

    /* Wait for Commands. */
    (void)printf("Press any key to exit the application. \r\n");
    (void)getchar();

destroy_end:
    IoTHubClient_Destroy(iotHubClientHandle);
end:
    platform_deinit();
}
