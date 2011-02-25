-module(cbloader_core).
-export([dispatch/1]).

-define(SHELL_WIDTH, 50).
-define(SHELL_UPDATE, 500).
-define(TCP_OPTS, [binary, {packet, line}, {active, false}]).

%% Main entry point
-spec dispatch(list()) -> ok.
dispatch(Conf) ->

    Self = self(),

    Servers = pget(servers, Conf),
    KeySize = pget(payload_size, Conf),
    NumKeys = pget(num_keys, Conf),

    log("Posting ~p keys of ~p bytes to each server:~n~n", [NumKeys, KeySize]),
    {Time, ok} = timer:tc(fun start/4, [Self, Servers, KeySize, NumKeys]),
    log("~nCompleted in ~.2f seconds~n", [Time/1000/1000]).


%% @doc Spin off a thread for each tcp connection and start writing data
%% immediately, start a timer for refreshing the ui and for collecting stats
-spec start(pid(), list(), integer(), integer()) -> ok.
start(Self, Servers, KeySize, NumKeys) ->

    Value = gen_bin(KeySize),

    {ok, Connections} = open_conn(Servers),
    {ok, DisplayTimer} = timer:send_interval(?SHELL_UPDATE, Self, print),
    {ok, StatsTimer} = timer:send_interval(?SHELL_UPDATE, Self, stats),

    [{SHost, SIP}|_Rest] = Servers,
    {ok, SSock} = gen_tcp:connect(SHost, SIP, ?TCP_OPTS),

    Workers = spawn_workers(Connections, Self, Value, NumKeys),
    Dict = dict:from_list([{Sock, {Host, IP, 0}} || {Host, IP, Sock} <- Workers]),

    wait(Dict, NumKeys, fetch_stats(SSock), SSock, length(Servers)),

    {ok, cancel} = timer:cancel(StatsTimer),
    {ok, cancel} = timer:cancel(DisplayTimer),
    ok = close_conn(Connections).


%% @doc Part of the main process, sits and waits for the other processes to
%% finish writing to memcache, fetches stats and updates ui in the meanwhile,
%% bit messy
wait(Workers, Total, Stats, Sock, NumServers) ->
    receive
        stats ->
            NStats = fetch_stats(Sock),
            wait(Workers, Total, NStats, Sock, NumServers);
        print ->
            output(Workers, Total, Stats, false),
            wait(Workers, Total, Stats, Sock, NumServers);
        {left, Pid, N} ->
            Fun = fun({Host, IP, _}) -> {Host, IP, N} end,
            wait(dict:update(Pid, Fun, Workers), Total, Stats, Sock, NumServers);
        {complete, _Pid} ->
            % Let my message queue flush
            case NumServers =:= 1 of
                true -> self() ! done;
                _    -> ok
            end,
            wait(Workers, Total, Stats, Sock, NumServers-1);
        done ->
            output(Workers, Total, Stats, true)
    end.

%% @doc format the currest status of individual server writes
write_status(Total, Left, Host, IP) ->
    Perc = (Total - Left) / Total,
    FullPerc = erlang:round(Perc * 100),
    fmt("=> ~s:~p ~s ~p% (~p/~p)~n",
        [Host, IP, pbar(Perc), FullPerc, Total - Left + 1, Total]).


%% @doc Generate a percentage bar [====   ] as a percentage of a fixed width
pbar(Perc) ->
    ToFill = erlang:round(?SHELL_WIDTH * Perc),
    ["[", lists:duplicate(ToFill, ["="]) ++
     lists:duplicate(?SHELL_WIDTH - ToFill, [" "]), "]"].


%% @doc Displays the current status in the shell
output(Workers, Total, Stats, LastLine) ->

    Work = [ write_status(Total, Left, Host, IP)
             || {_Pid, {Host, IP, Left}} <- dict:to_list(Workers) ],

    StatOut = stat(Stats, []),
    BackLog = length(Work) + length(StatOut) + 1,

    log([Work, StatOut]),

    % Set the cursor back to where I started writing
    % to do in place updates (unless its the last line)
    not LastLine andalso
        bash_back(BackLog).


%% @doc Write values to memcache in a busy loop, will need to configure with
%% pauses etc at some point, notify Main process when complete
cb_write(_Socks, Main, _Val, 0) ->
    Main ! {complete, self()},
    ok;

cb_write(Sock, Self, Val, N) ->
    %Cmd = fmt("set ~s ~p 0 ~p\r\n", [gen_key(), byte_size(Val) ]),
    ok = gen_tcp:send(Sock, cmd(gen_key(), byte_size(Val))),
    ok = gen_tcp:send(Sock, [Val, <<"\r\n">>]),
    {ok, <<"STORED\r\n">>} = gen_tcp:recv(Sock, 0, 500),
    Self ! {left, self(), N},
    cb_write(Sock, Self, Val, N-1).


%% @doc spawn a process for each server and start writing immediately
spawn_workers(Connections, Self, Value, NumKeys) ->
    [{Host, IP, spawn_link(fun() ->
                                   cb_write(Sock, Self, Value, NumKeys)
                           end)}
      || {Host, IP, Sock} <- Connections ].


%% @doc open a tcp socket for each server
open_conn(Servers) ->
    {ok, [begin
              {ok, Sock} = gen_tcp:connect(Host, IP, ?TCP_OPTS),
              {Host, IP, Sock}
          end || {Host, IP} <- Servers]}.

%% @doc close up sockets
close_conn(Connections) ->
    [ok = gen_tcp:close(Sock) || {_Host, _Ip, Sock} <- Connections],
    ok.


%% Fetch the stats via the memcached bucket
fetch_stats(Sock) ->
    ok = gen_tcp:send(Sock, fmt("stats\r\n", [])),
    collect_stats(Sock, []).

%% @doc Make sure to collect till END of stats, the format into proplist
collect_stats(Sock, Acc) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, <<"STAT ", Stat/binary>>} ->
            [Key, Val] = string:tokens(binary_to_list(Stat), " "),
            TStat = {list_to_atom(Key), trim_whitespace(Val)},
            collect_stats(Sock, [TStat|Acc]);
        {ok, <<"END\r\n">>} ->
            Acc
    end.


%% @doc filter and format the list of stats
stat([], Acc) ->
    [fmt("~nStatistics: ~n", []) | Acc];

stat([{bytes_written, Bytes} | Rest], _Acc) ->
    stat(Rest, [fmt("  Bytes Written: ~s~n", [Bytes])]);

stat([_Ignore | Rest], Acc) ->
    stat(Rest, Acc).


%% @doc trims newlines etc
trim_whitespace(Str) ->
    re:replace(Str, "\\s+", "", [global, {return, list}]).


%% @doc prints the terminal escape characters to go back X lines
bash_back(X) ->
    log("\e[" ++ integer_to_list(X) ++ "A").


%% @doc possibly the worse way to generate a binary value, works for now
gen_bin(N) ->
    <<0:N/integer-unit:8>>.
    %list_to_binary(lists:flatten(lists:duplicate(N, [$A])) ++ "\r\n").


%% @doc Generate an incremental key (this may screw up system time?)
gen_key() ->
    {Mega, Sec, Micro} = erlang:now(),
    i2l((Mega * 1000000 + Sec) * 1000000 + Micro).


%% @doc Create a set command to write to memcached
cmd(Key, Size) ->
    ["set ", Key, " 0 0 ", i2l(Size), "\r\n"].


%% Just a bunch of shorthand functions
fmt(Str, Args) ->
    lists:flatten(io_lib:format(Str, Args)).


pget(Key, List) ->
    proplists:get_value(Key, List).


log(Str) ->
    io:format(Str).
log(Str, Args) ->
    io:format(Str, Args).


i2l(Int) ->
    integer_to_list(Int).
