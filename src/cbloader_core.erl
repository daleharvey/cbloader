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
    KeyGen  = pget(key_gen, Conf),

    log("Posting ~p keys of ~p bytes to each server:~n~n", [NumKeys, KeySize]),
    {Time, ok} = timer:tc(fun start/5, [Self, Servers, KeySize, NumKeys, KeyGen]),
    log("~nCompleted in ~.2f seconds~n", [Time/1000/1000]).


%% @doc Spin off a thread for each tcp connection and start writing data
%% immediately, start a timer for refreshing the ui and for collecting stats
-spec start(pid(), list(), integer(), integer(), atom()) -> ok.
start(Self, Servers, KeySize, NumKeys, KeyGen) ->

    Value = gen_bin(KeySize),

    {ok, Connections} = open_conn(Servers),
    {ok, DisplayTimer} = timer:send_interval(?SHELL_UPDATE, Self, print),
    {ok, StatsTimer} = timer:send_interval(?SHELL_UPDATE, Self, stats),

    [{SHost, SIP}|_Rest] = Servers,
    {ok, SSock} = gen_tcp:connect(SHost, SIP, ?TCP_OPTS),

    Workers = spawn_workers(Connections, Self, Value, NumKeys, KeyGen),
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
cb_write(_Socks, Main, _Val, 0, _KeyGen) ->
    Main ! {complete, self()},
    ok;

cb_write(Sock, Self, Val, N, KeyGen) ->
    Key = gen_key(KeyGen, N),
    Cmd = fmt("set ~s ~p 0 ~p\r\n", [Key, 0, length(Val)]),
    ok = gen_tcp:send(Sock, Cmd),
    ok = gen_tcp:send(Sock, fmt("~s\r\n", [Val])),
    {ok, _Data} = gen_tcp:recv(Sock, 0),
    Self ! {left, self(), N},
    cb_write(Sock, Self, Val, N-1, KeyGen).


%% @doc spawn a process for each server and start writing immediately
spawn_workers(Connections, Self, Value, NumKeys, KeyGen) ->
    [{Host, IP, spawn(fun() -> cb_write(Sock, Self, Value, NumKeys, KeyGen) end)}
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
    lists:flatten(lists:duplicate(N, [$A])).


gen_key(incremental, N) ->
    "test-" ++ i2l(N);

gen_key(random, _N) ->
    [random:uniform(90) + $\s + 1 || _ <- lists:seq(1, 36)].


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
