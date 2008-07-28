-module(mysqlerl_port_sup).
-author('bjc@kublai.com').

-behavior(supervisor).

-export([start_link/7]).
-export([init/1]).

start_link(Cmd, Host, Port, Database, User, Password, Options) ->
    supervisor:start_link(?MODULE, [Cmd, Host, Port, Database,
                                    User, Password, Options]).

init([Cmd, Host, Port, Database, User, Password, Options]) ->
    Ref = {mysqlerl_port, {mysqlerl_port, start_link,
                            [Cmd, Host, Port, Database,
                             User, Password, Options]},
            transient, 5, worker, [mysqlerl_port]},
    {ok, {{one_for_one, 10, 5}, [Ref]}}.
