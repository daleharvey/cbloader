-module(cbloader_core).
-export([dispatch/1]).

%% Main entry point
-spec dispatch(list()) -> ok.
dispatch(Conf) ->

    Servers = proplists:get_value(servers, Conf),
    KeySize = proplists:get_value(payload_size, Conf),
    NumKeys = proplists:get_value(num_keys, Conf),

    cbloader_srv:start_link(),
    cbloader_srv:setup(Servers, KeySize, NumKeys),

    T1 = now(),

    cbloader_srv:start(),

    receive
        finished ->
            cbloader_srv:stop(),
            io:format("~n\e[32mCompleted in ~.2f seconds\e[0m~n",
                      [timer:now_diff(now(), T1)/1000000])
    end.
