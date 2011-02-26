-module(cbloader_core).
-export([dispatch/1]).

-define(SHELL_WIDTH, 50).
-define(SHELL_REFRESH, 500).
-define(STATS_UPDATE, 500).

-define(TCP_TIMEOUT, 5000).
-define(TCP_OPTS, [binary, {packet, line}]).

%% Main entry point
-spec dispatch(list()) -> ok.
dispatch(Conf) ->

    Self = self(),

    Servers = pget(servers, Conf),
    KeySize = pget(payload_size, Conf),
    NumKeys = pget(num_keys, Conf),

    {Time, ok} = timer:tc(fun start/4, [Self, Servers, KeySize, NumKeys]),
    log("~n\e[32mCompleted in ~.2f seconds\e[0m~n", [Time/1000/1000]).


%% @doc Spin off a thread for each tcp connection and start writing data
%% immediately, start a timer for refreshing the ui and for collecting stats
-spec start(pid(), list(), integer(), integer()) -> ok.
start(Self, Servers, KeySize, NumKeys) ->

    Value = gen_bin(KeySize),

    {ok, Connections} = open_conn(Servers),

    log("~nKeys: ~p ~nPacket Size: ~p~n~n", [NumKeys, KeySize]),

    {ok, DisplayTimer} = timer:send_interval(?SHELL_REFRESH, Self, print),
    {ok, StatsTimer} = timer:send_interval(?STATS_UPDATE, Self, stats),

    [{SHost, SIP}|_Rest] = Servers,
    {ok, SSock} = gen_tcp:connect(SHost, SIP, ?TCP_OPTS),

    Workers = spawn_workers(Connections, Self, Value, NumKeys),
    Dict = dict:from_list([{Sock, {Host, IP, 0}} || {Host, IP, Sock} <- Workers]),

    wait(Dict, NumKeys, dict:new(), SSock, length(Servers), dict:new()),

    {ok, cancel} = timer:cancel(StatsTimer),
    {ok, cancel} = timer:cancel(DisplayTimer),

    ok = close_conn(Connections).


%% @doc Part of the main process, sits and waits for the other processes to
%% finish writing to memcache, fetches stats and updates ui in the meanwhile,
%% bit messy, rewrite to gen_server
wait(Workers, Total, Stats, Sock, NumServers, Errs) ->
    wait(Workers, Total, Stats, Sock, NumServers, Errs, 0, NumServers * Total).

wait(Workers, Total, Stats, Sock, NumServers, Errs, C, X) ->
    receive
        stats ->
            ok = fetch_stats(Sock),
            wait(Workers, Total, Stats, Sock, NumServers, Errs, C, X);
        print ->
            output(Workers, Total, Stats, Errs, false),
            wait(Workers, Total, Stats, Sock, NumServers, Errs, C, X);
        {error, Err} ->
            NErrs = dict:update_counter(format_err(Err), 1, Errs),
            wait(Workers, Total, Stats, Sock, NumServers, NErrs, C, X);
        {left, Pid, N} ->
            Fun = fun({Host, IP, _}) -> {Host, IP, N} end,
            NDict = dict:update(Pid, Fun, Workers),
            wait(NDict, Total, Stats, Sock, NumServers, Errs, C, X);
        {complete, _Pid} ->
            % Let my message queue flush
            case NumServers =:= 1 of
                true -> self() ! done;
                _    -> ok
            end,
            wait(Workers, Total, Stats, Sock, NumServers-1, Errs, C, X);

        {tcp, _Port, <<"STORED\r\n">>} ->
            NStats = dict:update_counter(responses, 1, Stats),
            wait(Workers, Total, NStats, Sock, NumServers, Errs, C+1, X);
        {tcp, _Port, <<"STAT ", Rest/binary>>} ->
            [Key, Val] = string:tokens(binary_to_list(Rest), " "),
            NStats = dict:store(list_to_atom(Key), rm_trailing_rn(Val), Stats),
            wait(Workers, Total, NStats, Sock, NumServers, Errs, C, X);
        {tcp, _Port, <<"END\r\n">>} ->
            wait(Workers, Total, Stats, Sock, NumServers, Errs, C+1, X);
        {tcp, _Port, Err} ->
            NErrs = dict:update_counter(format_err(Err), 1, Errs),
            wait(Workers, Total, Stats, Sock, NumServers, NErrs, C+1, X);

        done ->
            Test = wait_for_x(X - C, []),
            NStats = dict:update_counter(responses, length(Test), Stats),
            output(Workers, Total, NStats, Errs, true),
            ok
        end.


%% @docs Wait for X number of messages to be sent and return them
wait_for_x(0, Acc) ->
    Acc;
wait_for_x(X, Acc) ->
    receive
        Msg ->
            recieveN(X - 1, [Msg | Acc])
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
output(Workers, Total, Stats, Errs, LastLine) ->

    ErrOut = [ fmt("\e[2K  \e[31m~s(~p)\e[0m~n", [ErrLabel, Count])
               || {ErrLabel, Count} <- dict:to_list(Errs) ],

    Work = [ write_status(Total, Left, Host, IP)
             || {_Pid, {Host, IP, Left}} <- dict:to_list(Workers) ],

    NErrors = case ErrOut of
                  [] -> [];
                  _ -> ["\e[2K\r\n", fmt("\e[2KErrors:~n", []) | ErrOut]
              end,

    StatOut = dict:fold(fun format_stats/3, [fmt("\e[2KStatistics: ~n", []),
                                             "\e[2K\r\n"], Stats),
    BackLog = length(NErrors) + length(Work) + length(StatOut),

    log([Work, lists:reverse(StatOut), NErrors]),
    % Set the cursor back to where I started writing
    % to do in place updates (unless its the last line)
    not LastLine andalso
        bash_back(BackLog).


%% Pick out some stats we want and format them, ignore the rest
format_stats(responses, Resp, Acc) ->
    [fmt("\e[2K  Responces: ~p~n", [Resp]) | Acc];
format_stats(bytes_written, Resp, Acc) ->
    [fmt("\e[2K  Bytes Written: ~s~n", [Resp]) | Acc];
format_stats(Key, Val, Acc) ->
    Acc.


%% @doc Write values to memcache in a busy loop, will need to configure with
%% pauses etc at some point, notify Main process when complete
cb_write(_Socks, Main, _Val, 0) ->
    Main ! {complete, self()},
    ok;

cb_write(Sock, Main, Val, N) ->
    ok = gen_tcp:send(Sock, cmd(gen_key(), byte_size(Val))),
    ok = gen_tcp:send(Sock, [Val, <<"\r\n">>]),
    Main ! {left, self(), N},
    cb_write(Sock, Main, Val, N-1).


%% @doc If memcached returns anything other than the expected stored
%% command, report back to main thread
report_errors(<<"STORED\r\n">>, _Main) ->
    ok;
report_errors(Err, Main) ->
    Main ! {error, Err},
    ok.

%% @doc spawn a process for each server and start writing immediately
spawn_workers(Connections, Self, Value, NumKeys) ->
    [{Host, IP, spawn_link(fun() ->
                                   cb_write(Sock, Self, Value, NumKeys)
                           end)}
      || {Host, IP, Sock} <- Connections ].


%% @doc open a tcp socket for each server
open_conn(Servers) ->
    {ok, [begin
              log("Connecting to ~s:~p~n", [Host, Port]),
              {ok, Sock} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
              {Host, Port, Sock}
          end || {Host, Port} <- Servers]}.

%% @doc close up sockets
close_conn(Connections) ->
    [ok = gen_tcp:close(Sock) || {_Host, _Ip, Sock} <- Connections],
    ok.


%% Fetch the stats via the memcached bucket
fetch_stats(Sock) ->
    ok = gen_tcp:send(Sock, fmt("stats\r\n", [])).
    %collect_stats(Sock, []).

%% @doc Make sure to collect till END of stats, the format into proplist
collect_stats(Sock, Acc) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, <<"STAT ", Stat/binary>>} ->
            [Key, Val] = string:tokens(binary_to_list(Stat), " "),
            TStat = {list_to_atom(Key), rm_trailing_rn(Val)},
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


%% @doc make errors readable
format_err(Err) ->
    rm_trailing_rn(binary_to_list(Err)).


%% @doc trims trailing newlines
rm_trailing_rn(Str) ->
    "\n\r" ++ Rest = lists:reverse(Str),
    lists:reverse(Rest).


%% @doc prints the terminal escape characters to go back X lines
bash_back(X) ->
    log("\e[" ++ integer_to_list(X) ++ "A").


%% @doc any reason to generate different data?
gen_bin(N) ->
    <<0:N/integer-unit:8>>.


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
