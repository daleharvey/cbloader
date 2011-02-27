-module(cbloader).
-export([main/1]).

%% @doc Called automatically by escript
-spec main(list()) -> ok.
main(Args) ->
    Opts = parse(Args, []),
    Def = parse(string:tokens(defaults(), " "), []),
    run(merge(Def, Opts)).


%% @doc default command line parameters
defaults() ->
    "-n 10k -v 1kb -p 5 -s 127.0.0.1:12001".


%% @doc Merge 2 proplists into each other, taking all the key from
%% Prop1 and overwriting any values that exist in Prop2
merge(Prop1, Prop2) ->
    merge(Prop1, Prop2, []).

merge([], _Opts, Acc) ->
    Acc;
merge([{Key, Def} | Rest], Opts, Acc) ->
    Val = case lists:keyfind(Key, 1, Opts) of
              {Key, V} -> V;
              _Else    -> Def
          end,
    merge(Rest, Opts, [{Key, Val} | Acc]).


%% @doc Parse the command line arguments (in the form --key val) into
%% a proplist, exapand shorthand args
parse([], Acc) ->
    Acc;

%% @doc processes, number of worker processes to spin off for each server
parse(["-p", Keys | Rest], Acc) ->
    parse(["--processes", Keys | Rest], Acc);
parse(["--processes", Keys | Rest], Acc) ->
    parse(Rest, [{processes, list_to_integer(Keys)} | Acc]);

%% @doc num_keys, the number of keys to write to memcached
parse(["-n", Keys | Rest], Acc) ->
    parse(["--num-keys", Keys | Rest], Acc);
parse(["--num-keys", Keys | Rest], Acc) ->
    parse(Rest, [{num_keys, to_num(Keys)} | Acc]);

%% @doc payload_size, the size of the values to write to memcached
parse(["-v", Size | Rest], Acc) ->
    parse(["--value-size", Size | Rest], Acc);
parse(["--value-size", Size | Rest], Acc) ->
    parse(Rest, [{value_size, to_bytes(Size)} | Acc]);

%% @doc servers, the list of servers to write to
parse(["-s", Servers | Rest], Acc) ->
    parse(["--servers", Servers | Rest], Acc);
parse(["--servers", Servers | Rest], Acc) ->
    parse(Rest, [parse_server(Servers) | Acc]).


%% @doc parse shorthand number formats, (5k = 5 thousand, 5m = 5 million)
to_num(X) ->
    {N, Rest} = string:to_integer(X),
    to_num(N, string:to_lower(Rest)).

to_num(N, []) ->
    N;
to_num(N, "k") ->
    N * 1000;
to_num(N, "m") ->
    N * 1000000.


%% @doc parse shorthand storage formats, (1k, 10mb)
to_bytes(X) ->
    {N, Rest} = string:to_integer(X),
    to_bytes(N, string:to_lower(Rest)).

to_bytes(N, []) ->
    N;
to_bytes(N, "kb") ->
    N * 1024;
to_bytes(N, "mb") ->
    N * 1024 * 1024.

%% @doc parse the server string into a list of hosts + ip mappings
parse_server(Servers) ->
    {servers,
     [begin
          [Host, Port] = string:tokens(Server, ":"),
          {Host, list_to_integer(Port)}
      end || Server <- string:tokens(Servers, ",")]}.


%% @doc run this baby
run(Conf) ->
    try cbloader_core:dispatch(Conf)
    catch Type:Error ->
            io:format("Error running script:~n~p~n~p~n",
                [erlang:get_stacktrace(), {Type, Error}])
    end.
