-module(brod_consumer_prop).

-behavior(brod_topic_subscriber).

-include("brod_proper.hrl").

%% Proper callbacks
-export([ command/1
        , initial_state/0
        , next_state/3
        , precondition/2
        , postcondition/3
        ]).

%% brod_topic_subscriber callbacks
-export([ init/2
        , handle_message/3
        ]).

%% Test actions
-export([ produce/3
        , receive_messages/3
        , start_subscriber/2
        , start_client/2
        , stop_subscriber/2
        , crash_consumer/1
        ]).

-export([compactify/1]).

%%% ---------------------------------------------------------------------------
%%% Definitions:
%%% ---------------------------------------------------------------------------


-define(MESSAGE_DEADLINE, 8000).   %% Milliseconds

-define(RESTART_DEADLINE, 2).       %% Seconds

-define(MAX_MESSAGES, 10).

-define(BEGIN_OFFSET, earliest).

-record(s,
        { client_id           :: atom()
        , generation_id       :: binary()
        , subscriber
        , subscriber_config
        , produced_id = 255
        , processed_id = 255
        , last_check = {?BEGIN_OFFSET, undefined}
        }).

-define(TEST_ACTION(Fun), {call, ?MODULE, Fun, []}).
-define(TEST_ACTION(Fun, A), {call, ?MODULE, Fun, [A]}).
-define(TEST_ACTION(Fun, A, B), {call, ?MODULE, Fun, [A, B]}).
-define(TEST_ACTION(Fun, A, B, C), {call, ?MODULE, Fun, [A, B, C]}).
-define(TEST_ACTION(Fun, A, B, C, D), {call, ?MODULE, Fun, [A, B, C, D]}).

-define(PRODUCE(Gen, Id, NMsgs), ?TEST_ACTION(produce, Gen, Id, NMsgs)).

-define(COLLECT(Gen, SubPid, LastExpected), ?TEST_ACTION(receive_messages, Gen, SubPid, LastExpected)).

-define(START_CLIENT(ClientId, Config), ?TEST_ACTION(start_client, ClientId, Config)).

-define(START_TOPIC_SUBSCRIBER(State, Config), ?TEST_ACTION(start_subscriber, State, Config)).

-define(STOP_TOPIC_SUBSCRIBER(Pid, StopType), ?TEST_ACTION(stop_subscriber, Pid, StopType)).

-define(CRASH_CONSUMER(State), ?TEST_ACTION(crash_consumer, State)).

-define(ACK(SubPid, GenId, Partition, Msg, Offset),
        {ack, SubPid, GenId, Partition, Msg, Offset}).

%%% ---------------------------------------------------------------------------
%%% Testcase entry point:
%%% ---------------------------------------------------------------------------
prop_test() ->
  process_flag(trap_exit, true),
  ?FORALL(Cmds, commands(?MODULE),
          begin
            {History, State, Result} = run_commands(?MODULE, Cmds),
            ?WHENFAIL( io:format( "History: ~p\nState: ~p\nResult: ~p\n"
                                , [History,State,Result]
                                )
                     , aggregate(command_names(Cmds), Result =:= ok))
          end).

%%% ---------------------------------------------------------------------------
%%% Test callbacks:
%%% ---------------------------------------------------------------------------

initial_state() ->
  %% Tag every kafka message with an ID specific for particular
  %% testcase run to avoid cross-contamination:
  #s{ generation_id = term_to_binary(make_ref())
    }.

command(#s{client_id = undefined}) ->
  ?START_CLIENT(gensym(client), client_config_gen());
command(#s{client_id = Client, subscriber = undefined} = State)
  when Client /= undefined ->
  ?START_TOPIC_SUBSCRIBER(State, subscriber_config_gen());
command(#s{ generation_id = Gen
          , produced_id = ProdID
          , processed_id = DoneID
          , subscriber = SubPid
          } = State) ->
  frequency(
    [ {10, ?PRODUCE(Gen, ProdID + 1, range(1, ?MAX_MESSAGES))}
    , {2,  ?COLLECT(Gen, SubPid, ProdID)}
    %, {1,  ?STOP_TOPIC_SUBSCRIBER(State#s.subscriber, stop_type())}
    , {5,  ?CRASH_CONSUMER(State)}
    ]).

produce(GenId, MsgId, NMsgs) ->
  io:format(user, "<~p", [MsgId + NMsgs]),
  lists:foreach( fun(I) ->
                   brod:produce( ?TEST_CLIENT_ID
                               , ?TEST_TOPIC
                               , 0
                               , GenId
                               , term_to_binary(I)
                               )
                 end
               , lists:seq(MsgId, MsgId + NMsgs)
               ).

start_subscriber(State, Config) ->
  {Offset, _} = State#s.last_check,
  Self = self(),
  {ok, Pid} = brod:start_link_topic_subscriber( State#s.client_id
                                              , ?TEST_TOPIC
                                              , Config
                                              , ?MODULE
                                              , {State#s.generation_id, Self, Offset}
                                              ),
  error_logger:info_msg( "Started subscriber ~p from offset ~p~n"
                         "Config: ~p~nIn state ~p~n"
                       , [Pid, Offset, Config, State]
                       ),
  %%timer:sleep(timer:seconds(?RESTART_DEADLINE)),
  Pid.

stop_subscriber(Pid, normal) ->
  io:format(user, "s", []),
  brod_topic_subscriber:stop(Pid);
stop_subscriber(Pid, crash) ->
  io:format(user, "x", []),
  exit(Pid, simuated_error).

crash_consumer(#s{ client_id = ClientId
                 , subscriber = Subscriber
                 , last_check = {LastOffset, _}
                 , subscriber_config = Cfg
                 } = State) ->
  io:format(user, "*", []),
  {ok, Consumer} = brod_client:get_consumer(ClientId, ?TEST_TOPIC, 0),
  exit(Consumer, simulated_error),
  timer:sleep(timer:seconds(?RESTART_DEADLINE)),
  case is_process_alive(Subscriber) of
    true ->
      %% Topic subscriber survived consumer restart, carry on
      Subscriber;
    false ->
      %% Restart subscriber
      NewSubscriber = start_subscriber(State, Cfg),
      error_logger:info_msg( "Subscriber ~p got killed as well, restarting as ~p~n"
                           , [Subscriber, NewSubscriber]
                           ),
      NewSubscriber
  end.

start_client(ClientId, Config) ->
  io:format(user, "~n==========================================~n", []),
  Res = brod:start_client(?BOOTSTRAP_HOSTS, ClientId, Config),
  Res.

receive_messages(Gen, SubPid, LastExpected) ->
  TRef = erlang:start_timer(?MESSAGE_DEADLINE, self(), Gen),
  FlushMessages =
    fun F({LastOffset, Acc} = OldState) ->
        receive
          ?ACK(SubPid, Gen, _, Payload, Offset) ->
            SeqNo = binary_to_term(Payload),
            error_logger:info_msg("~p: Got msg:~p/~p~n", [ ?MODULE
                                                         , binary_to_term(Gen)
                                                         , SeqNo]),
            Ret = {Offset, [SeqNo | Acc]},
            if SeqNo >= LastExpected ->
                Ret;
               true ->
                F(Ret)
            end;
          ?ACK(OtherPid, _, _, Payload, Offset) ->
            SeqNo = binary_to_term(Payload),
            error_logger:info_msg("~p: discarded ack message from ~p (current=~p)~n"
                                  "~p at offset ~p~n",
                                  [ ?MODULE, OtherPid, SubPid, SeqNo, Offset]),
            F(OldState);
          {timeout, TRef, _} ->
            error_logger:warning_msg( "~p:Timeout waiting for messages: ~p~n"
                                    , [?MODULE, OldState]
                                    ),
            OldState
        end
    end,
  {Offset, Messages} = FlushMessages({undefined, []}),
  ok = flush_cancel_timer(TRef),
  error_logger:info_msg("All messages: ~p~n", [Messages]),
  io:format(user, "{~p->~p}", [hd(Messages), Offset]),
  {Offset, compactify(lists:reverse(Messages))}.

flush_cancel_timer(TRef) ->
  erlang:cancel_timer(TRef),
  receive
    {timeout, Tref, _} ->
      ok
  after
    0 ->
      ok
  end.

precondition(#s{last_check = Check}, ?CRASH_CONSUMER(_)) ->
  %% This precondition is not entirely justified; it's here to avoid
  %% corner case in the model
  Check /= {?BEGIN_OFFSET, undefined};
precondition(#s{processed_id = From, produced_id = To}, ?COLLECT(_, _, _)) ->
  From < To;
precondition(_State, _Command) ->
  true.

postcondition(#s{processed_id = From, produced_id = To}, ?COLLECT(_, _, _), {_Offset, Messages}) ->
  Expected = [{From + 1, To}],
  case Messages of
    [{FromE, To}] when
        %% From =:= 255, FromE >= From;            % Initially we start subscriber with
        %%                                         % offset=latest, so first few messages
        %%                                         % may be lost in the first COLLECT
        FromE =:= From + 1 ->
      io:format(user, "v[~p]", [To - From - 1]),
      true;
    _ ->
      error_logger:error_msg( "Mismatch:~nGot:~p~nExpected:~p~n~n"
                            , [Messages, Expected]
                            ),
      false
  end;
postcondition(_State, ?START_CLIENT(ClientId, _), Res) ->
  [Pid] = brod_sup:find_client(ClientId),
  is_process_alive(Pid);
postcondition(_State, ?CRASH_CONSUMER(_), Res) ->
  is_process_alive(Res);
postcondition(_State, _Command, _Res) ->
  true.

next_state(State, _Res, ?START_CLIENT(ClientId, _)) ->
  State#s{client_id = ClientId};
next_state(State, Res, ?START_TOPIC_SUBSCRIBER(_, Config)) ->
  State#s{subscriber = Res, subscriber_config = Config};
next_state(State, _Res, ?PRODUCE(_, N, NMsgs)) ->
  State#s{produced_id = N + NMsgs};
next_state(State, Res, ?COLLECT(_, _, _)) ->
  State#s{processed_id = State#s.produced_id, last_check = Res};
next_state(State, _Res, ?STOP_TOPIC_SUBSCRIBER(_, _)) ->
  State#s{subscriber = undefined};
next_state(State, Res, ?CRASH_CONSUMER(_)) ->
  State#s{subscriber = Res};
next_state(State, _Res, _Command) ->
  State.

%%% ---------------------------------------------------------------------------
%%% Generators
%%% ---------------------------------------------------------------------------

stop_type() ->
  oneof([crash, normal]).

client_config_gen() ->
  [ {restart_delay_seconds, range(0, ?RESTART_DEADLINE div 2)}
  , {reconnect_cool_down_seconds, range(1, ?RESTART_DEADLINE div 2)}
  , {allow_topic_auto_creation, boolean()}
  , {auto_start_producers, boolean()}
  %% , {default_producer_config, producer_config_gen()}
  ] ++ brod_proper_SUITE:client_config().

subscriber_config_gen() ->
  ?LET({MinBytes, MaxBytes}, {range(0, 1 bsl 9), range(0, 1 bsl 9)},
       [ {min_bytes, MinBytes}
       , {max_bytes, MinBytes + MaxBytes}
       , {max_wait_time, range(0, ?MESSAGE_DEADLINE div 2)}
       , {sleep_timeout, range(0, ?MESSAGE_DEADLINE div 2)}
       , {prefetch_count, range(0, ?MAX_MESSAGES*8)}
       , {prefetch_bytes, range(0, 1 bsl 16)}
       , {size_stat_window, 5}
       , {begin_offset, ?BEGIN_OFFSET}
       ]).

%%% ---------------------------------------------------------------------------
%%% Brod callbacks
%%% ---------------------------------------------------------------------------

init(Topic, {GenId, Parent, BeginOffset}) ->
  CommittedOffsets = case BeginOffset of
                       ?BEGIN_OFFSET ->
                         [];
                       Num when is_integer(Num) ->
                         [{0, Num}]
                     end,
  error_logger:info_msg("~p:init(~p) -> ~p~n", [?MODULE, GenId, CommittedOffsets]),
  {ok, CommittedOffsets, {GenId, Parent}}.

handle_message(Partition, Msg, {GenId, ParentPid} = State) ->
  #kafka_message{value = Val, key = Key, offset = Offset} = Msg,
  if Key =:= GenId ->
      ParentPid ! ?ACK(self(), GenId, Partition, Val, Offset);
     true ->
      ok
  end,
  {ok, ack, State}.

%%% ---------------------------------------------------------------------------
%%% Utility functions
%%% ---------------------------------------------------------------------------

compactify([]) ->
  [];
compactify([A|T]) ->
  lists:reverse(
    lists:foldl( fun(I, [{Begin, End}|Acc]) ->
                     if I =:= End + 1 ->
                         [{Begin, I}|Acc];
                       true ->
                         [{I, I}, {Begin, End}|Acc]
                     end
                 end
               , [{A, A}]
               , T)).

%% Cursed function generating an unique atom. It's as harmful as it sounds
%% It allows to run tests in parallel
gensym(What) ->
  list_to_atom(atom_to_list(What) ++ "_" ++ integer_to_list(erlang:phash2(make_ref()))).

%% Troubleshooting:
%% brod_cli fetch --topic brod_consumer_prop_topic -c -1 --offset <offset> --partition 0 --fmt "io_lib:format(\"~p -> ~p~n\", [binary_to_term(Value), Offset])."


%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
