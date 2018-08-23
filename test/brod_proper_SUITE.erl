-module(brod_proper_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("brod_proper.hrl").

-define(DBG_PROCESS_FLAGS, [call, m, return_to, p, sos]).

-define(TRACE, true).

-define(TIMEOUT, 600).
-define(NUMTESTS, 100).
-define(SIZE, 1000).

-define(RUN_PROP(PROP),
        begin
          io:format(??PROP),
          Result = proper:quickcheck( PROP
                                    , [ {numtests, ?NUMTESTS}
                                      , {max_size, ?SIZE}
                                      , noshrink
                                      ]
                                      ),
          true = Result
        end).

suite() ->
  [{timetrap,{seconds,?TIMEOUT}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(sasl),
  %% ok = application:set_env(sasl, sasl_error_logger, {file, "prop_test.log"}),
  {ok, _} = application:ensure_all_started(brod),
  ProducerConfig = [{required_acks, 0}],
  ClientConfig = [ {auto_start_producers, true}
                 , {default_producer_config, ProducerConfig}
                 ] ++ client_config(),
  ok = brod:start_client(?BOOTSTRAP_HOSTS, ?TEST_CLIENT_ID, ClientConfig),
  %% Give brod some time to initialize itself:
  timer:sleep(8000),
  Config.

end_per_suite(_Config) ->
  ok.

init_per_testcase(_TestCase, Config) ->
  register(tc_prop_handler, self()),
  ?TRACE andalso start_tracer(),
  Config.

end_per_testcase(_TestCase, _Config) ->
  ?TRACE andalso ttb:stop(),
  ok.

all() ->
  [my_test_case].

my_test_case(Config) ->
  ?RUN_PROP(brod_consumer_prop:prop_test()).

children(SupId) ->
  [whereis(SupId)] ++ [element(2, I) || I <- supervisor3:which_children(SupId)].

client_config() ->
  case os:getenv("KAFKA_VERSION") of
    "0.9" ++ _ -> [{query_api_versions, false}];
    _ -> []
  end.

start_tracer() ->
  ttb:tracer(node(), [ {process_info, true}
                     %% , {handler, ttb:get_et_handler()}
                     ]),
  %%{ok, _} = ttb:p(new_processes, ?DBG_PROCESS_FLAGS),
  TracedPids = [self()] ++ children(brod_sup),
  [ttb:p(Pid, ?DBG_PROCESS_FLAGS) || Pid <- TracedPids],
  ttb:tp(brod_consumer, []),
  ttb:tp(brod, []),
  ttb:tp(brod_client, find_client, []),
  ttb:tp(brod_topic_subscriber, []),
  ttb:tp(brod_consumer_prop, []).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
