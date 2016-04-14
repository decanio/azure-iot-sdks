// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdio.h>
#include <getopt.h>
#include "iothub_client_suri_amqp.h"

#define OPT_BATCH    1


int main(int argc, char **argv)
{
    struct option long_opts[] = {
        {"batch", optional_argument, NULL, 'b'},
        {NULL, 0, NULL, 0}
    };
    char short_opts[] = "b";
    int opt;
    int option_index = 0;
    int batch = 0;
    int i = 0;

    while ((opt = getopt_long(argc, argv, short_opts, long_opts, &option_index)) != -1) {
        printf("option '%c':%d\n", (char)opt, opt);
        switch (opt) {
        case 'b': 
            batch = 1;
            i++;
            break;
        }
    }
    i++;
    printf("done option_index is %d\n", i);
    if (argc >= i) {
        printf("taking eve-log input from %s\n", argv[i]);
        iothub_client_suri_amqp_run(batch, argv[i]);
    }
    return 0;
}
