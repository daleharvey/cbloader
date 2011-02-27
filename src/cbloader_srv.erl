-module(cbloader_srv).

-behaviour(gen_server).
-define(SERVER, ?MODULE).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-define(SHELL_WIDTH, 50).
-define(SHELL_REFRESH, 500).
-define(STATS_UPDATE, 500).


-define(TCP_TIMEOUT, 5000).
-define(TCP_OPTS, [binary, {packet, line}]).


-record(state, {
          connections = [],
          parent = null,
          servers = [],
          workers = [],
          display_timer,
          stats_timer,
          num_keys = 0,
          key_size = 0,
          stats = dict:new(),
          stats_socket = null,
          completed = 0,
          errors = dict:new(),
          recieved = 0
         }).

-export([start_link/0, stop/0, setup/4, start/0]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

setup(Parent, Servers, KeySize, NumKeys) ->
    gen_server:call(?MODULE, {set_data, Parent, Servers, KeySize, NumKeys}).

start() ->
    gen_server:call(?MODULE, start).

stop() ->
    gen_server:cast(?MODULE, stop).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.


handle_call({set_data, Parent, Servers, KeySize, NumKeys}, _From, _State) ->
    {reply, ok, #state{parent=Parent, servers=Servers,
                       key_size=KeySize, num_keys=NumKeys}};

handle_call(start, _From, State) ->
    {reply, ok, do_start(State)};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(stop, #state{stats_timer=StatsTimer, display_timer=DisplayTimer,
                         connections=Connections} = State) ->

    {ok, cancel} = timer:cancel(StatsTimer),
    {ok, cancel} = timer:cancel(DisplayTimer),

    ok = close_conn(Connections),

    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(print, #state{workers=Workers, num_keys=Total,
                          stats=Stats, errors=Errs} = State) ->
    output(Workers, Total, Stats, Errs, false),
    {noreply, State};

handle_info(stats, #state{stats_socket=Sock} = State) ->
    ok = fetch_stats(Sock),
    {noreply, State};

handle_info({error, Err}, #state{errors = Errs} = State) ->
    NErrs = dict:update_counter(format_err(Err), 1, Errs),
    {noreply, State#state{errors = NErrs}};

handle_info({left, Pid, N}, #state{workers=Workers} = State) ->
    NDict = dict:update(Pid, fun({Host, IP, _}) -> {Host, IP, N} end, Workers),
    {noreply, State#state{workers = NDict}};

handle_info({complete, _Pid}, #state{servers=Servers,
                                            completed=Completed} = State) ->
    case Completed + 1 =:= length(Servers) of
        true -> self() ! done;
        _    -> ok
    end,
    {noreply, State#state{completed = Completed + 1}};

handle_info({tcp, _, <<"STORED\r\n">>}, #state{recieved=Recv,
                                               stats=Stats} = State) ->
    {noreply, State#state{recieved=Recv + 1,
                            stats = dict:update_counter(responses, 1, Stats)}};

handle_info({tcp, _, <<"STAT ", Rest/binary>>}, #state{stats=Stats} = State) ->
    [Key, Val] = string:tokens(binary_to_list(Rest), " "),
    NStats = dict:store(list_to_atom(Key), rm_trailing_rn(Val), Stats),
    {noreply, State#state{stats=NStats}};


handle_info({tcp, _Port, <<"END\r\n">>}, State) ->
    {noreply, State};

handle_info({tcp, _, Err}, #state{recieved=Recv, errors=Errors} = State) ->
    NErrs = dict:update_counter(format_err(Err), 1, Errors),
    {noreply, State#state{recieved=Recv + 1, errors = NErrs}};

handle_info(done, #state{stats=Stats, num_keys=NumKeys, servers=Servers,
                         workers=Workers, errors=Errors, recieved=C,
                         parent = Parent} = State) ->
    Test = wait_for_x((length(Servers) * NumKeys) - C, []),
    NStats = dict:update_counter(responses, length(Test), Stats),
    output(Workers, NumKeys, NStats, Errors, true),
    Parent ! finished,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

do_start(#state{servers=Servers, key_size=KeySize, num_keys=NumKeys} = State) ->

    Value = gen_bin(KeySize),

    {ok, Connections} = open_conn(Servers),

    log("~nKeys: ~p ~nPacket Size: ~p~n~n", [NumKeys, KeySize]),

    {ok, DisplayTimer} = timer:send_interval(?SHELL_REFRESH, self(), print),
    {ok, StatsTimer} = timer:send_interval(?STATS_UPDATE, self(), stats),

    [{SHost, SIP}|_Rest] = Servers,
    {ok, SSock} = gen_tcp:connect(SHost, SIP, ?TCP_OPTS),

    Workers = spawn_workers(Connections, self(), Value, NumKeys),
    Dict = dict:from_list([{Sock, {Host, IP, 0}} || {Host, IP, Sock} <- Workers]),

    State#state{
      connections = Connections,
      workers = Dict,
      display_timer = DisplayTimer,
      stats_timer = StatsTimer,
      stats_socket = SSock
     }.

%% @docs Wait for X number of messages to be sent and return them
wait_for_x(0, Acc) ->
    Acc;
wait_for_x(X, Acc) ->
    receive
        Msg ->
            wait_for_x(X - 1, [Msg | Acc])
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
format_stats(_Key, _Val, Acc) ->
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


log(Str) ->
    io:format(Str).
log(Str, Args) ->
    io:format(Str, Args).


i2l(Int) ->
    integer_to_list(Int).
