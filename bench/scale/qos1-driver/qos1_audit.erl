%% Measurement extension for the pinned emqtt-bench 0.6.3 image.
%% A bitmap per topic and subscriber process reconciles a shared group offline.
-module(qos1_audit).
-export([start/0, enabled/0, paused/0, stopped/0, record/3, ack/1, latency/1, init/2]).
enabled() -> persistent_term:get(qos1_enabled, false).
start() ->
    persistent_term:put(qos1_enabled, os:getenv("QOS1_AUDIT") =:= "1"),
    case enabled() of
        false -> ok;
        true ->
            %% VM-wide collectors can suspend/survey thousands of client
            %% processes. Keep only workload metrics on the measurement path;
            %% host CPU is sampled independently by mpstat.
            Keep = [prometheus_boolean, prometheus_counter, prometheus_gauge,
                    prometheus_histogram, prometheus_quantile_summary, prometheus_summary],
            [prometheus_registry:deregister_collector(C)
             || C <- prometheus_registry:collectors(default), not lists:member(C, Keep)],
            prometheus_counter:declare([{name,audit_negative_latency},{help,"Negative cross-host latency observations"}]),
            prometheus_counter:inc(audit_negative_latency, 0),
            ets:new(qos1_ledger, [named_table, public, set, {write_concurrency, true}]),
            persistent_term:put(qos1_paused, false),
            [prometheus_counter:declare([{name,N},{help,atom_to_list(N)}]) || N <- [audit_sent,audit_acked,audit_received]],
            prometheus_gauge:declare([{name, audit_paused_workers}, {help, "Stopped publication workers"}]),
            prometheus_histogram:declare([{name, puback_latency}, {help, "Synchronous publish completion ms"},
                {buckets, [1,5,10,25,50,100,500,1000,5000]}])
    end.
paused() -> enabled() andalso persistent_term:get(qos1_paused, false).
stopped() -> prometheus_gauge:inc(audit_paused_workers).
record(Kind, Topic, <<_:64, Seq:64, _/binary>>) ->
    case enabled() of
        false -> ok;
        true when Seq < 10000000 ->
            prometheus_counter:inc(case Kind of sent -> audit_sent; acked -> audit_acked; received -> audit_received end),
            Key = {Kind, Topic, self()},
            {Bits, Count} = case ets:lookup(qos1_ledger, Key) of
                [] -> {0, 0}; [{_, B, C}] -> {B, C}
            end,
            ets:insert(qos1_ledger, {Key, Bits bor (1 bsl Seq), Count + 1}), ok;
        true -> error(sequence_out_of_range)
    end;
record(_, _, _) -> case enabled() of true -> error(missing_sequence_header); false -> ok end.
%% Negative samples must remain visible even when positive samples dominate
%% the sum. Do not clamp them into the fastest histogram bucket.
latency(Value) ->
    case enabled() andalso Value < 0 of
        true -> prometheus_counter:inc(audit_negative_latency), false;
        false -> true
    end.
ack(Start) ->
    case enabled() of true -> prometheus_histogram:observe(puback_latency,
        (erlang:monotonic_time(microsecond) - Start) / 1000); false -> ok end.
init(Req, State) ->
    Path = cowboy_req:path(Req), Method = cowboy_req:method(Req),
    {Status, Body} = case {enabled(), Method, Path} of
        {true, <<"POST">>, <<"/audit/pause">>} ->
            persistent_term:put(qos1_paused, true), {200, <<"paused\n">>};
        {true, <<"GET">>, <<"/audit/ledger">>} ->
            Rows = [io_lib:format("~s\t~s\t~s\t~B\t~s~n", [atom_to_list(K),
                base64:encode(T), pid_to_list(P), C, integer_to_binary(B,16)])
                || {{K,T,P},B,C} <- ets:tab2list(qos1_ledger)],
            {200, [Rows, "# EOF\n"]};
        _ -> {404, <<"not found\n">>}
    end,
    {ok, cowboy_req:reply(Status, #{<<"content-type">> => <<"text/plain">>}, Body, Req), State}.
