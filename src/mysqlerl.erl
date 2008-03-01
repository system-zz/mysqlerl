%% Modeled from ODBC
%% http://www.erlang.org/doc/apps/odbc/

-module(mysqlerl).
-author('bjc@kublai.com').

-include("mysqlerl.hrl").

-export([test_start/0, test_msg/0]).

-export([start/0, start/1, stop/0, commit/2, commit/3,
         connect/6, disconnect/1, describe_table/2,
         describe_table/3, first/1, first/2,
         last/1, last/2, next/1, next/2, prev/1,
         prev/2, select_count/2, select_count/3,
         select/3, select/4, param_query/3, param_query/4,
         sql_query/2, sql_query/3]).

-define(CONFIG, "/Users/bjc/tmp/test-server.cfg").

test_start() ->
    {ok, [{Host, Port, DB, User, Pass, Options}]} = file:consult(?CONFIG),
    mysqlerl:connect(Host, Port, DB, User, Pass, Options).

test_msg() ->
    commit(mysqlerl_connection_sup:random_child(),
           rollback, 2000).

start() ->
    start(temporary).

%% Arguments:
%%     Type = permanent | transient | temporary
%%
%% Returns:
%%     ok | {error, Reason}
start(Type) ->
    application:start(sasl),
    application:start(mysqlerl, Type).

stop() ->
    application:stop(mysqlerl).

commit(Ref, CommitMode) ->
    commit(Ref, CommitMode, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%%     CommitMode = commit | rollback
%%     Reason = not_an_explicit_commit_connection |
%%              process_not_owner_of_odbc_connection |
%%              common_reason()
%%     ok | {error, Reason}
commit(Ref, commit, Timeout) ->
    gen_server:call(Ref, #sql_commit{}, Timeout);
commit(Ref, rollback, Timeout) ->
    gen_server:call(Ref, #sql_rollback{}, Timeout).

%% Arguments:
%%     Host = string()
%%     Port = integer()
%%     Database = string()
%%     User = string()
%%     Password = string()
%%     Options = list()
%%
%% Returns:
%%     {ok, Ref} | {error, Reason}
%%     Ref = connection_reference()
connect(Host, Port, Database, User, Password, Options) ->
    mysqlerl_connection_sup:connect(Host, Port, Database,
                                    User, Password, Options).

%% Arguments:
%%     Ref = connection_reference()
%%
%% Returns:
%%     ok | {error, Reason}
disconnect(Ref) ->
    mysqlerl_connection:stop(Ref).

describe_table(Ref, Table) ->
    describe_table(Ref, Table, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Table = string()
%%     Timeout = time_out()
%%
%% Returns:
%%     {ok, Description} | {error, Reason}
%%     Description = [{col_name(), odbc_data_type()}]
describe_table(Ref, Table, Timeout) ->
    gen_server:call(Ref, #sql_describe_table{table = Table}, Timeout).

first(Ref) ->
    first(Ref, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
first(Ref, Timeout) ->
    gen_server:call(Ref, #sql_first{}, Timeout).

last(Ref) ->
    last(Ref, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
last(Ref, Timeout) ->
    gen_server:call(Ref, #sql_last{}, Timeout).

next(Ref) ->
    next(Ref, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
next(Ref, Timeout) ->
    gen_server:call(Ref, #sql_next{}, Timeout).

prev(Ref) ->
    prev(Ref, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
prev(Ref, Timeout) ->
    gen_server:call(Ref, #sql_prev{}, Timeout).

select_count(Ref, SQLQuery) ->
    select_count(Ref, SQLQuery, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     SQLQuery = string()
%%     Timeout = time_out()
%% Returns:
%%     {ok, NrRows} | {error, Reason}
%%     NrRows = n_rows()
select_count(Ref, SQLQuery, Timeout) ->
    gen_server:call(Ref, #sql_select_count{q = SQLQuery}, Timeout).

select(Ref, Pos, N) ->
    select(Ref, Pos, N, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Pos = integer()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
select(Ref, Pos, N, Timeout) ->
    gen_server:call(Ref, #sql_select{pos = Pos, n = N}, Timeout).

param_query(Ref, SQLQuery, Params) ->
    param_query(Ref, SQLQuery, Params, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     SQLQuery = string()
%%     Params = [{odbc_data_type(), [value()]}]
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
param_query(Ref, SQLQuery, Params, Timeout) ->
    gen_server:call(Ref, #sql_param_query{q = SQLQuery, params = Params},
                    Timeout).

sql_query(Ref, SQLQuery) ->
    sql_query(Ref, SQLQuery, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     SQLQuery = string()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
sql_query(Ref, SQLQuery, Timeout) ->
    gen_server:call(Ref, #sql_query{q = SQLQuery}, Timeout).
