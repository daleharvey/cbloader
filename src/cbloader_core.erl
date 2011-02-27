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

    Servers = pget(servers, Conf),
    KeySize = pget(payload_size, Conf),
    NumKeys = pget(num_keys, Conf),

    cbloader_srv:start_link(),
    cbloader_srv:setup(self(), Servers, KeySize, NumKeys),

    T1 = now(),

    cbloader_srv:start(),

    receive
        finished ->
            cbloader_srv:stop(),
            log("~n\e[32mCompleted in ~.2f seconds\e[0m~n",
                [timer:now_diff(now(), T1)/1000000])
    end.


pget(Key, List) ->
    proplists:get_value(Key, List).


log(Str) ->
    io:format(Str).
log(Str, Args) ->
    io:format(Str, Args).
