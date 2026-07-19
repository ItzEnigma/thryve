#ifndef THRYVE_H
#define THRYVE_H

/* Generated with cbindgen:0.29.4 */

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum {
  FfiStatus_Ok = 0,
  FfiStatus_NullArg = 1,
  FfiStatus_AllocFail = 2,
  FfiStatus_InvalidState = 3,
  FfiStatus_OperationFail = 4,
  FfiStatus_StrConvError = 5,
} FfiStatus;

typedef uint8_t *FfiTask;

typedef struct {
  const char *result_type;
  const char *message;
  const char *log_file;
  bool has_chain;
  FfiTask chain_task;
} FfiTaskResult;

typedef uint8_t *FfiConfig;

typedef uint8_t *FfiPacketHandle;

typedef uint8_t *FfiCtx;

typedef uint8_t *FfiPlugin;

typedef uint8_t *FfiObserver;

typedef uint8_t *FfiNotif;

typedef FfiTaskResult (*FfiTaskJob)(void);

typedef uint8_t *FfiRunHandle;

typedef uint8_t *FfiService;

typedef struct {
  FfiService service;
  FfiCtx ctx;
} FfiServiceCreateResult;

typedef void (*FfiPluginCleanUp)(const void*);

typedef FfiTaskResult (*FfiPluginEntryPoint)(void);

typedef void (*FfiObserverCleanUp)(const void*);

typedef void (*FfiObsJob)(FfiObserver);

typedef void (*FfiObserverMainLoop)(const void*);

typedef void (*FfiNotifLogic)(const void*);

FfiTaskResult ffi_task_result_new(const char *result_type,
                                  const char *message,
                                  const char *log_file);

FfiTaskResult ffi_task_result_new_with_chain(const char *result_type,
                                             const char *message,
                                             const char *log_file,
                                             FfiTask chain_task);

FfiConfig ffi_config_default(void);

FfiConfig ffi_config_from_file(const char *path);

void ffi_config_free(FfiConfig config);

void ffi_packet_handle_free(FfiPacketHandle handle);

FfiPacketHandle ffi_packet_handle_clone(FfiPacketHandle handle);

FfiCtx ffi_ctx_create(FfiConfig config);

void ffi_ctx_free(FfiCtx ctx);

FfiStatus ffi_ctx_add_plugin(FfiCtx ctx, FfiPlugin plugin);

FfiStatus ffi_ctx_add_observer(FfiCtx ctx, FfiObserver observer);

FfiStatus ffi_ctx_set_logging_file_names(FfiCtx ctx, const char *const *file_names, int32_t count);

FfiStatus ffi_ctx_send_task(FfiCtx ctx, FfiTask task);

FfiPacketHandle ffi_ctx_get_packet_handle(FfiCtx ctx);

FfiStatus ffi_packet_handle_send_task(FfiPacketHandle handle, FfiTask task);

FfiStatus ffi_packet_handle_send_notification(FfiPacketHandle handle, FfiNotif notif);

FfiTask ffi_task_create(const char *name, uint8_t priority, FfiTaskJob job);

void ffi_task_free(FfiTask task);

FfiRunHandle ffi_run_interactive_start(FfiCtx ctx);

FfiStatus ffi_run_interactive_free(FfiRunHandle handle);

FfiServiceCreateResult ffi_service_create(FfiConfig config);

FfiStatus ffi_service_run(FfiService service, FfiCtx ctx);

void ffi_service_free(FfiService service);

FfiPlugin ffi_plugin_create(const char *plg_name,
                            const char *plg_type,
                            uint8_t *plg_core_ptr,
                            FfiPacketHandle tx,
                            FfiPluginCleanUp clean_up,
                            FfiPluginEntryPoint entry_point);

void ffi_plugin_free(FfiPlugin plugin);

FfiObserver ffi_observer_create(const char *obs_name,
                                const char *obs_type,
                                uint8_t *obs_core_ptr,
                                FfiPacketHandle tx,
                                FfiObserverCleanUp clean_up,
                                FfiObsJob obs_job_fn);

void ffi_observer_free(FfiObserver observer);

FfiNotif ffi_notification_create(const char *notif_type,
                                 const char *notif_receiver,
                                 uint8_t *notif_core_ptr);

void ffi_notification_free(FfiNotif notif);

FfiStatus ffi_ctx_send_notification(FfiCtx ctx, FfiNotif notif);

#endif  /* THRYVE_H */
