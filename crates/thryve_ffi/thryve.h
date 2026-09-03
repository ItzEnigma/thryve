/*
 * Thryve FFI — generated C header. Do not edit by hand.
 *
 * Copyright (c) 2026 Youssef Mostafa Abdellatif
 *
 * This file is part of Thryve, dual-licensed under the GNU Affero General
 * Public License v3.0 (or later) and a separate Commercial License.
 * See LICENSE and COMMERCIAL.md in the Thryve repository root, or visit
 * https://thryve-framework.com for licensing options.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef THRYVE_H
#define THRYVE_H

/* Generated with cbindgen:0.29.4 */

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum {
  ThryveStatus_Ok = 0,
  ThryveStatus_NullArg = 1,
  ThryveStatus_AllocFail = 2,
  ThryveStatus_InvalidState = 3,
  ThryveStatus_OperationFail = 4,
  ThryveStatus_StrConvError = 5,
} ThryveStatus;

typedef struct ConfigHandle ConfigHandle;

typedef struct CtxHandle CtxHandle;

typedef struct NotifHandle NotifHandle;

typedef struct ObserverHandle ObserverHandle;

typedef struct PacketHandleHandle PacketHandleHandle;

typedef struct PluginHandle PluginHandle;

typedef struct RunHandleHandle RunHandleHandle;

typedef struct ServiceHandle ServiceHandle;

typedef struct TaskHandle TaskHandle;

typedef ConfigHandle *ThryveConfig;

typedef CtxHandle *ThryveCtx;

typedef RunHandleHandle *ThryveRunHandle;

typedef ServiceHandle *ThryveService;

typedef PluginHandle *ThryvePlugin;

typedef ObserverHandle *ThryveObserver;

typedef PacketHandleHandle *ThryvePacketHandle;

typedef TaskHandle *ThryveTask;

typedef NotifHandle *ThryveNotif;

typedef struct {
  const char *result_type;
  const char *message;
  const char *log_file;
  bool has_chain;
  ThryveTask chain_task;
} ThryveTaskResult;

typedef struct {
  ThryveService service;
  ThryveCtx ctx;
} ThryveServiceCreateResult;

typedef ThryveTaskResult (*ThryveTaskJob)(void);

typedef void (*ThryvePluginCleanUp)(const void*);

typedef ThryveTaskResult (*ThryvePluginEntryPoint)(void);

typedef void (*ThryveObserverCleanUp)(const void*);

typedef void (*ThryveObserverMainLoop)(const void*);

typedef void (*ThryveObsJob)(ThryveObserver);

typedef void (*ThryveNotifLogic)(const void*);

#endif  /* THRYVE_H */
