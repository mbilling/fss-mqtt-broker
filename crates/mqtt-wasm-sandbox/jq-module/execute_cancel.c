/*
 * EMQX's jq interpreter (jqc's src/execute.c, compiled from the pinned source as it
 * is), and one function more: where a program keeps the flag jq_cancel sets.
 *
 * shim.c hands that address to the host through the cancel slot (mqttd_cancel_slot).
 * At a call's deadline the host stores 1 there, which is jq_cancel without running
 * any of the module's code; the interpreter looks at the flag at its next step. The
 * runtime cannot call into the module while a call is running (RUSTSEC-2026-0362), so
 * the host cannot call jq_cancel itself.
 */
#include "src/execute.c" /* through -Ijqc */

volatile int *mqttd_jq_cancel_flag(jq_state *jq) { return &jq->canceled; }
