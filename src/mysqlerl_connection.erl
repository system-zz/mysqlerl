-module(mysqlerl_connection).
-author('bjc@kublai.com').

-behavior(gen_server).

-export([start_link/6, stop/1, sql_query/3]).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-define(QUERY_MSG, 0).
-define(COMMIT_MSG, 1).
-define(ROLLBACK_MSG, 2).
-define(EXTENDED_MSG, 255).

-record(state, {ref}).
-record(port_closed, {reason}).
-record(sql_query, {q}).

start_link(Host, Port, Database, User, Password, Options) ->
    gen_server:start_link(?MODULE, [Host, Port, Database,
                                    User, Password, Options], []).

stop(Pid) ->
    gen_server:cast(Pid, stop).

sql_query(Pid, Query, Timeout) ->
    gen_server:call(Pid, #sql_query{q = Query}, Timeout).

init([Host, Port, Database, User, Password, Options]) ->
    process_flag(trap_exit, true),
    Cmd = lists:flatten(io_lib:format("~s ~s ~w ~s ~s ~s ~s",
                                      [helper(), Host, Port, Database,
                                       User, Password, Options])),
    Ref = open_port({spawn, Cmd}, [{packet, 4}, binary]),
    {ok, #state{ref = Ref}}.

terminate(#port_closed{reason = Reason}, #state{ref = Ref}) ->
    io:format("DEBUG: mysqlerl connection ~p shutting down (~p).~n",
              [Ref, Reason]),
    ok;
terminate(Reason, State) ->
    port_close(State#state.ref),
    io:format("DEBUG: got terminate: ~p~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(#sql_query{q = Query}, _From, State) ->
    {reply, handle_query(State#state.ref, Query), State};
handle_call(Request, From, State) ->
    io:format("DEBUG: got unknown call from ~p: ~p~n", [From, Request]),
    {noreply, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({'EXIT', _Ref, Reason}, State) ->
    {stop, #port_closed{reason = Reason}, State};
handle_info(Info, State) ->
    io:format("DEBUG: got unknown info: ~p~n", [Info]),
    {noreply, State}.

helper() ->
    case code:priv_dir(mysqlerl) of
        PrivDir when is_list(PrivDir) -> ok;
        {error, bad_name} -> PrivDir = filename:join(["..", "priv"])
    end,
    filename:join([PrivDir, "mysqlerl"]).

handle_query(Ref, Query) ->
    io:format("DEBUG: got query: ~p~n", [Query]),
    make_request(Ref, {sql_query, Query}).

make_request(Ref, Req) ->
    port_command(Ref, term_to_binary(Req)),
    receive
        {Ref, {data, Res}} -> {ok, Res};
        Other ->
            error_logger:warning_msg("Got unknown query response: ~p~n",
                                     [Other]),
            exit({badreply, Other})
    end.
