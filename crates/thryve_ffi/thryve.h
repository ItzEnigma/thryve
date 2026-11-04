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

#define THRYVE_ERROR_NO_OUTCOME -1

#define THRYVE_ERROR_WRONG_OUTCOME -2

typedef enum {
  ThryveStatus_Ok = 0,
  ThryveStatus_NullArg = 1,
  ThryveStatus_AllocFail = 2,
  ThryveStatus_InvalidState = 3,
  ThryveStatus_OperationFail = 4,
  ThryveStatus_StrConvError = 5,
} ThryveStatus;

typedef struct AdvHandleOpaque AdvHandleOpaque;

typedef struct AdvTaskOpaque AdvTaskOpaque;

typedef struct ConfigHandle ConfigHandle;

typedef struct CtxHandle CtxHandle;

typedef struct FanoutGroupOpaque FanoutGroupOpaque;

typedef struct HandoffOpaque HandoffOpaque;

typedef struct NotifHandle NotifHandle;

typedef struct ObserverHandle ObserverHandle;

typedef struct OutcomeOpaque OutcomeOpaque;

typedef struct PacketHandleHandle PacketHandleHandle;

typedef struct PluginHandle PluginHandle;

typedef struct RunHandleHandle RunHandleHandle;

typedef struct ServiceHandle ServiceHandle;

typedef struct TaskHandle TaskHandle;

typedef TaskHandle *ThryveTask;

typedef struct {
  const char *result_type;
  const char *message;
  const char *log_file;
  bool has_chain;
  ThryveTask chain_task;
} ThryveTaskResult;

typedef ConfigHandle *ThryveConfig;

typedef PacketHandleHandle *ThryvePacketHandle;

typedef CtxHandle *ThryveCtx;

typedef PluginHandle *ThryvePlugin;

typedef ObserverHandle *ThryveObserver;

typedef NotifHandle *ThryveNotif;

typedef void (*ThryveTaskJob)(ThryveTaskResult *result, void *user_data);

typedef void (*ThryveFreeFn)(void*);

typedef RunHandleHandle *ThryveRunHandle;

typedef ServiceHandle *ThryveService;

typedef struct {
  ThryveService service;
  ThryveCtx ctx;
} ThryveServiceCreateResult;

typedef int (*ThryvePluginCleanUp)(const void*);

typedef void (*ThryvePluginEntryPoint)(ThryveTaskResult *result, void *plg_core_ptr);

typedef void (*ThryveObserverCleanUp)(const void*);

typedef void (*ThryveObsJob)(ThryveObserver);

typedef OutcomeOpaque *ThryveOutcome;

typedef AdvHandleOpaque *ThryveAdvHandle;

typedef AdvTaskOpaque *ThryveAdvTask;

typedef ThryveOutcome (*ThryveAdvJob)(void*);

typedef HandoffOpaque *ThryveHandoff;

typedef ThryveOutcome (*ThryveThenJob)(ThryveHandoff, void*);

typedef ThryveOutcome (*ThryveSplitJob)(size_t, size_t, void*);

typedef ThryveOutcome (*ThryveChunkJob)(void*, size_t, void*);

typedef FanoutGroupOpaque *ThryveFanoutGroup;

const char *thryve_last_error(void);

ThryveTaskResult thryve_task_result_new(const char *result_type,
                                        const char *message,
                                        const char *log_file);

ThryveTaskResult thryve_task_result_new_with_chain(const char *result_type,
                                                   const char *message,
                                                   const char *log_file,
                                                   ThryveTask chain_task);

ThryveConfig thryve_config_default(void);

ThryveConfig thryve_config_from_file(const char *path);

void thryve_config_free(ThryveConfig config);

void thryve_packet_handle_free(ThryvePacketHandle handle);

ThryvePacketHandle thryve_packet_handle_clone(ThryvePacketHandle handle);

ThryveCtx thryve_ctx_create(ThryveConfig config);

void thryve_ctx_free(ThryveCtx ctx);

ThryveStatus thryve_ctx_add_plugin(ThryveCtx ctx, ThryvePlugin plugin);

ThryveStatus thryve_ctx_add_observer(ThryveCtx ctx, ThryveObserver observer);

ThryveStatus thryve_ctx_set_logging_file_names(ThryveCtx ctx,
                                               const char *const *file_names,
                                               int32_t count);

ThryveStatus thryve_ctx_send_task(ThryveCtx ctx, ThryveTask task);

ThryvePacketHandle thryve_ctx_get_packet_handle(ThryveCtx ctx);

ThryveStatus thryve_packet_handle_send_task(ThryvePacketHandle handle, ThryveTask task);

ThryveStatus thryve_packet_handle_send_notification(ThryvePacketHandle handle, ThryveNotif notif);

ThryveTask thryve_task_create(const char *name,
                              uint8_t priority,
                              ThryveTaskJob job,
                              void *user_data,
                              ThryveFreeFn free_user_data);

void thryve_task_free(ThryveTask task);

ThryveRunHandle thryve_run_interactive_start(ThryveCtx ctx);

ThryveStatus thryve_run_interactive_free(ThryveRunHandle handle);

ThryveServiceCreateResult thryve_service_create(ThryveConfig config);

ThryveStatus thryve_service_run(ThryveService service, ThryveCtx ctx);

void thryve_service_free(ThryveService service);

ThryvePlugin thryve_plugin_create(const char *plg_type,
                                  void *plg_core_ptr,
                                  ThryvePacketHandle tx,
                                  ThryvePluginCleanUp clean_up,
                                  ThryvePluginEntryPoint entry_point);

void thryve_plugin_free(ThryvePlugin plugin);

ThryveObserver thryve_observer_create(const char *obs_type,
                                      void *obs_core_ptr,
                                      ThryvePacketHandle tx,
                                      ThryveObserverCleanUp clean_up,
                                      ThryveObsJob obs_job_fn);

void thryve_observer_free(ThryveObserver observer);

ThryveNotif thryve_observer_pop(ThryveObserver observer);

void *thryve_observer_core(ThryveObserver observer);

ThryveNotif thryve_notification_create(const char *notif_type,
                                       const char *notif_receiver,
                                       void *notif_core_ptr);

void thryve_notification_free(ThryveNotif notif);

void *thryve_notification_core(ThryveNotif notif);

ThryveStatus thryve_ctx_send_notification(ThryveCtx ctx, ThryveNotif notif);

ThryveOutcome thryve_outcome_value(void *value, ThryveFreeFn free_value);

ThryveOutcome thryve_outcome_error(int32_t code, const char *message);

ThryveOutcome thryve_outcome_forward(ThryveAdvHandle handle);

void thryve_outcome_free(ThryveOutcome outcome);

ThryveAdvTask thryve_adv_task_create(const char *name,
                                     uint8_t priority,
                                     ThryveAdvJob job,
                                     void *user_data,
                                     ThryveFreeFn free_user_data);

ThryveAdvTask thryve_adv_task_create_forward(const char *name,
                                             uint8_t priority,
                                             ThryveAdvJob job,
                                             void *user_data,
                                             ThryveFreeFn free_user_data);

void thryve_adv_task_free(ThryveAdvTask task);

ThryveAdvHandle thryve_packet_handle_send_adv_task(ThryvePacketHandle handle, ThryveAdvTask task);

ThryveAdvHandle thryve_adv_handle_then(ThryveAdvHandle handle,
                                       const char *name,
                                       ThryveThenJob job,
                                       void *user_data,
                                       ThryveFreeFn free_user_data);

ThryveAdvHandle thryve_adv_handle_then_with_priority(ThryveAdvHandle handle,
                                                     const char *name,
                                                     uint8_t priority,
                                                     ThryveThenJob job,
                                                     void *user_data,
                                                     ThryveFreeFn free_user_data);

ThryveAdvHandle thryve_adv_handle_then_forward(ThryveAdvHandle handle,
                                               const char *name,
                                               ThryveThenJob job,
                                               void *user_data,
                                               ThryveFreeFn free_user_data);

void thryve_adv_handle_cancel(ThryveAdvHandle handle);

void thryve_adv_handle_free(ThryveAdvHandle handle);

ThryveAdvHandle thryve_adv_handles_join(ThryveAdvHandle *handles,
                                        size_t count,
                                        const char *name,
                                        ThryveThenJob job,
                                        void *user_data,
                                        ThryveFreeFn free_user_data);

ThryveAdvHandle thryve_adv_handles_join_with_priority(ThryveAdvHandle *handles,
                                                      size_t count,
                                                      const char *name,
                                                      uint8_t priority,
                                                      ThryveThenJob job,
                                                      void *user_data,
                                                      ThryveFreeFn free_user_data);

void thryve_adv_handles_cancel_all(ThryveAdvHandle *handles, size_t count);

ThryveStatus thryve_packet_handle_split_into(ThryvePacketHandle handle,
                                             size_t item_count,
                                             const char *name,
                                             size_t number_of_pieces,
                                             uint8_t priority,
                                             ThryveSplitJob job,
                                             void *user_data,
                                             ThryveFreeFn free_user_data,
                                             ThryveAdvHandle *out_handles,
                                             size_t *out_count);

ThryveStatus thryve_packet_handle_send_split_tasks(ThryvePacketHandle handle,
                                                   void *const *chunks,
                                                   size_t chunk_count,
                                                   ThryveFreeFn free_chunk,
                                                   const char *name,
                                                   uint8_t priority,
                                                   ThryveChunkJob job,
                                                   void *user_data,
                                                   ThryveFreeFn free_user_data,
                                                   ThryveAdvHandle *out_handles);

ThryveFanoutGroup thryve_adv_handle_fan_out(ThryveAdvHandle handle);

ThryveAdvHandle thryve_fanout_then(ThryveFanoutGroup group,
                                   const char *name,
                                   ThryveThenJob job,
                                   void *user_data,
                                   ThryveFreeFn free_user_data);

ThryveAdvHandle thryve_fanout_then_with_priority(ThryveFanoutGroup group,
                                                 const char *name,
                                                 uint8_t priority,
                                                 ThryveThenJob job,
                                                 void *user_data,
                                                 ThryveFreeFn free_user_data);

ThryveStatus thryve_fanout_send(ThryveFanoutGroup group);

bool thryve_handoff_is_error(ThryveHandoff handoff);

size_t thryve_handoff_value_count(ThryveHandoff handoff);

const void *thryve_handoff_value(ThryveHandoff handoff, size_t index);

void *thryve_handoff_take_value(ThryveHandoff handoff, size_t index);

int32_t thryve_handoff_error_code(ThryveHandoff handoff);

const char *thryve_handoff_error_message(ThryveHandoff handoff);

size_t thryve_handoff_error_position(ThryveHandoff handoff);

#endif  /* THRYVE_H */
