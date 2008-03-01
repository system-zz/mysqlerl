-module(mysqlerl_app).
-author('bjc@kublai.com').

-behavior(application).
-behavior(supervisor).

-export([start/2, stop/1, init/1]).

start(normal, []) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop([]) ->
    ok.

init([]) ->
    ConnectionSup = {mysqlerl_connection_sup, {mysqlerl_connection_sup, start_link, []},
                     permanent, infinity, supervisor, [mysqlerl_connection_sup]},
    {ok, {{one_for_one, 10, 5},
          [ConnectionSup]}}.
