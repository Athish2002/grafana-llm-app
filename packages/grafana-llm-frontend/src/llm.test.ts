import {
  enabled,
  extractContent,
  streamChatCompletions,
  ChatCompletionsResponse,
  ChatCompletionsChunk,
  accumulateToolCalls,
  accumulateStreamingToolCalls,
  accumulateChatTurn,
  recoverToolCallMessage,
  parseToolCallArguments,
  ToolCallsMessage,
} from "./llm";
import { LLM_PLUGIN_ID, LLM_PLUGIN_ROUTE } from "./constants";
import { getBackendSrv, getGrafanaLiveSrv } from "@grafana/runtime";
import { LiveChannelEventType } from "@grafana/data";
import { from, of } from "rxjs";

jest.mock("@grafana/runtime", () => ({
  getBackendSrv: jest.fn(),
  getGrafanaLiveSrv: jest.fn(),
}));

describe("enabled", () => {
  it("should return false if not configured", async () => {
    (getBackendSrv as jest.Mock).mockImplementation(() => ({
      get: jest.fn().mockReturnValue(Promise.resolve({ enabled: false })),
    }));

    // Call the enabled function
    const result = await enabled();
    expect(result).toBe(false);
  });

  it("should return true if configured", async () => {
    (getBackendSrv as jest.Mock).mockImplementation(() => ({
      get: jest.fn().mockImplementation((url: string) => {
        if (url === `${LLM_PLUGIN_ROUTE}/settings`) {
          return Promise.resolve({ enabled: true });
        } else if (url === `${LLM_PLUGIN_ROUTE}/health`) {
          return Promise.resolve({
            details: { llmProvider: { configured: true, ok: true } },
          });
        }
        // raise an error if we get here
        throw new Error("unexpected url");
      }),
    }));

    // Call the enabled function
    const result = await enabled();
    expect(result).toBe(true);
  });
});

describe("streamChatCompletions", () => {
  it("subscribes via a channel carrying both stream and namespace", () => {
    const getStream = jest.fn().mockReturnValue(of());
    (getGrafanaLiveSrv as jest.Mock).mockReturnValue({ getStream });

    streamChatCompletions({ model: "large", messages: [] });

    // Grafana >= 12.4 builds the channel id from `stream`; older versions use
    // `namespace`. Both must be set so the subscribe routes on every version.
    const channel = getStream.mock.calls[0][0];
    expect(channel.stream).toBe(LLM_PLUGIN_ID);
    expect(channel.namespace).toBe(LLM_PLUGIN_ID);
    expect(channel.path).toMatch(/^llm\/v1\/chat\/completions\//);
  });
});

describe("extractContent", () => {
  it("should handle empty choices array without throwing", (done) => {
    const emptyResponse: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "test-id",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [], // Empty choices array
      usage: {
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
      },
    };

    const source$ = of(emptyResponse);
    const result$ = source$.pipe(extractContent());

    const results: string[] = [];
    result$.subscribe({
      next: (value) => results.push(value),
      error: (err) => {
        // Should not error
        done(err);
      },
      complete: () => {
        // Should complete successfully with no emitted values
        expect(results).toEqual([]);
        done();
      },
    });
  });

  it("should extract content from valid content messages", (done) => {
    const validResponse: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "test-id",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            content: "Hello, world!",
            role: "assistant" as const,
          },
        },
      ],
      usage: {
        prompt_tokens: 10,
        completion_tokens: 5,
        total_tokens: 15,
      },
    };

    const source$ = of(validResponse);
    const result$ = source$.pipe(extractContent());

    const results: string[] = [];
    result$.subscribe({
      next: (value) => results.push(value),
      error: (err) => {
        done(err);
      },
      complete: () => {
        expect(results).toEqual(["Hello, world!"]);
        done();
      },
    });
  });
});

describe("streamChatCompletions finish_reason termination", () => {
  it("terminates when finish_reason is 'tool_calls'", (done) => {
    const events = [
      {
        type: LiveChannelEventType.Message,
        message: {
          id: "test-1",
          object: "chat.completion.chunk",
          created: Date.now(),
          model: "test-model",
          choices: [
            {
              delta: {
                role: "assistant" as const,
                tool_calls: [
                  {
                    id: "call_1",
                    index: 0,
                    type: "function" as const,
                    function: {
                      name: "get_weather",
                      arguments: '{"location":"Paris"}',
                    },
                  },
                ],
              },
              index: 0,
            },
          ],
          usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
        },
      },
      {
        type: LiveChannelEventType.Message,
        message: {
          id: "test-2",
          object: "chat.completion.chunk",
          created: Date.now(),
          model: "test-model",
          choices: [
            {
              delta: {},
              finish_reason: "tool_calls",
              index: 0,
            },
          ],
          usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
        },
      },
      {
        type: LiveChannelEventType.Message,
        message: {
          id: "test-3",
          object: "chat.completion.chunk",
          created: Date.now(),
          model: "test-model",
          choices: [
            {
              delta: { content: "should never be reached" },
              index: 0,
            },
          ],
          usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
        },
      },
    ];

    const getStream = jest.fn().mockReturnValue(from(events));
    (getGrafanaLiveSrv as jest.Mock).mockReturnValue({ getStream });

    const results: any[] = [];
    streamChatCompletions({ model: "base", messages: [] }).subscribe({
      next: (val) => results.push(val),
      error: (err) => done(err),
      complete: () => {
        expect(results.length).toBe(1);
        expect(results[0].id).toBe("test-1");
        done();
      },
    });
  });

  it("terminates when finish_reason is 'length'", (done) => {
    const events = [
      {
        type: LiveChannelEventType.Message,
        message: {
          id: "test-1",
          object: "chat.completion.chunk",
          created: Date.now(),
          model: "test-model",
          choices: [
            {
              delta: { content: "partial content" },
              index: 0,
            },
          ],
          usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
        },
      },
      {
        type: LiveChannelEventType.Message,
        message: {
          id: "test-2",
          object: "chat.completion.chunk",
          created: Date.now(),
          model: "test-model",
          choices: [
            {
              delta: {},
              finish_reason: "length",
              index: 0,
            },
          ],
          usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
        },
      },
      {
        type: LiveChannelEventType.Message,
        message: {
          id: "test-3",
          object: "chat.completion.chunk",
          created: Date.now(),
          model: "test-model",
          choices: [
            {
              delta: { content: "unreachable" },
              index: 0,
            },
          ],
          usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
        },
      },
    ];

    const getStream = jest.fn().mockReturnValue(from(events));
    (getGrafanaLiveSrv as jest.Mock).mockReturnValue({ getStream });

    const results: any[] = [];
    streamChatCompletions({ model: "base", messages: [] }).subscribe({
      next: (val) => results.push(val),
      error: (err) => done(err),
      complete: () => {
        expect(results.length).toBe(1);
        expect(results[0].id).toBe("test-1");
        done();
      },
    });
  });
});

describe("accumulateToolCalls and recoverToolCallMessage", () => {
  it("accumulates single tool call split across chunks", (done) => {
    const chunk1: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "1",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            tool_calls: [
              {
                id: "call_abc",
                index: 0,
                type: "function",
                function: { name: "get_weather", arguments: '{"location":' },
              },
            ],
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
    const chunk2: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "2",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            tool_calls: [
              {
                id: "call_abc",
                index: 0,
                type: "function",
                function: { name: "", arguments: ' "London"}' },
              },
            ],
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };

    from([chunk1, chunk2])
      .pipe(accumulateToolCalls())
      .subscribe({
        next: (result) => {
          expect(result.role).toBe("assistant");
          expect(result.tool_calls).toHaveLength(1);
          expect(result.tool_calls[0]).toEqual({
            id: "call_abc",
            index: 0,
            type: "function",
            function: {
              name: "get_weather",
              arguments: '{"location": "London"}',
            },
          });
        },
        error: (err) => done(err),
        complete: () => done(),
      });
  });

  it("handles multiple parallel tool calls delivered out-of-order", (done) => {
    // Deliver index 1 before index 0
    const chunk1: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "1",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            tool_calls: [
              {
                id: "call_2",
                index: 1,
                type: "function",
                function: { name: "get_time", arguments: '{"zone":' },
              },
            ],
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
    const chunk2: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "2",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            tool_calls: [
              {
                id: "call_1",
                index: 0,
                type: "function",
                function: { name: "get_", arguments: "" },
              },
            ],
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
    const chunk3: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "3",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            tool_calls: [
              {
                id: "call_1",
                index: 0,
                type: "function",
                function: { name: "weather", arguments: '{"city":"Berlin"}' },
              },
              {
                id: "call_2",
                index: 1,
                type: "function",
                function: { name: "", arguments: ' "UTC"}' },
              },
            ],
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };

    from([chunk1, chunk2, chunk3])
      .pipe(accumulateToolCalls())
      .subscribe({
        next: (result) => {
          expect(result.tool_calls).toHaveLength(2);
          // Keyed and sorted by index: 0 first, 1 second
          expect(result.tool_calls[0]).toEqual({
            id: "call_1",
            index: 0,
            type: "function",
            function: {
              name: "get_weather",
              arguments: '{"city":"Berlin"}',
            },
          });
          expect(result.tool_calls[1]).toEqual({
            id: "call_2",
            index: 1,
            type: "function",
            function: {
              name: "get_time",
              arguments: '{"zone": "UTC"}',
            },
          });
        },
        error: (err) => done(err),
        complete: () => done(),
      });
  });

  it("recovers tool calls when index is omitted without throwing TypeError", () => {
    const rawChunks: ToolCallsMessage[] = [
      {
        role: "assistant",
        tool_calls: [
          {
            id: "call_no_idx",
            type: "function",
            function: { name: "search", arguments: "" },
          },
        ],
      },
      {
        role: "assistant",
        tool_calls: [
          {
            id: "",
            type: "function",
            function: { name: "", arguments: '{"q":"grafana"}' },
          },
        ],
      },
    ];

    const recovered = recoverToolCallMessage(rawChunks);
    expect(recovered.tool_calls).toHaveLength(1);
    expect(recovered.tool_calls[0].index).toBe(0);
    expect(recovered.tool_calls[0].id).toBe("call_no_idx");
    expect(recovered.tool_calls[0].function.name).toBe("search");
    expect(recovered.tool_calls[0].function.arguments).toBe('{"q":"grafana"}');
  });

  it("defaults empty arguments to '{}'", () => {
    const rawChunks: ToolCallsMessage[] = [
      {
        role: "assistant",
        tool_calls: [
          {
            id: "call_empty",
            index: 0,
            type: "function",
            function: { name: "ping", arguments: "" },
          },
        ],
      },
    ];

    const recovered = recoverToolCallMessage(rawChunks);
    expect(recovered.tool_calls[0].function.arguments).toBe("{}");
  });
});

describe("accumulateStreamingToolCalls", () => {
  it("emits progressive running tool call updates on each chunk", (done) => {
    const chunk1: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "1",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            tool_calls: [
              {
                id: "call_prog",
                index: 0,
                type: "function",
                function: { name: "exec", arguments: '{"cmd":' },
              },
            ],
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
    const chunk2: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "2",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            tool_calls: [
              {
                id: "call_prog",
                index: 0,
                type: "function",
                function: { name: "", arguments: ' "ls"}' },
              },
            ],
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };

    const emissions: ToolCallsMessage[] = [];
    from([chunk1, chunk2])
      .pipe(accumulateStreamingToolCalls())
      .subscribe({
        next: (msg) => emissions.push(msg),
        error: (err) => done(err),
        complete: () => {
          expect(emissions).toHaveLength(2);
          expect(emissions[0].tool_calls[0].function.arguments).toBe('{"cmd":');
          expect(emissions[1].tool_calls[0].function.arguments).toBe(
            '{"cmd": "ls"}',
          );
          done();
        },
      });
  });
});

describe("accumulateChatTurn", () => {
  it("handles mixed content and tool calls over a single subscription", (done) => {
    const chunk1: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "1",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            content: "Checking status. ",
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
    const chunk2: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "2",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            content: "Please wait.",
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
    const chunk3: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "3",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {
            role: "assistant",
            tool_calls: [
              {
                id: "call_check",
                index: 0,
                type: "function",
                function: { name: "status_check", arguments: '{"all": true}' },
              },
            ],
          },
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
    const chunk4: ChatCompletionsResponse<ChatCompletionsChunk> = {
      id: "4",
      object: "chat.completion.chunk",
      created: Date.now(),
      model: "test-model",
      choices: [
        {
          delta: {},
          finish_reason: "tool_calls",
          index: 0,
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };

    const snapshots: any[] = [];
    from([chunk1, chunk2, chunk3, chunk4])
      .pipe(accumulateChatTurn())
      .subscribe({
        next: (snap) => snapshots.push(snap),
        error: (err) => done(err),
        complete: () => {
          expect(snapshots).toHaveLength(4);

          // Snapshot 1: content only
          expect(snapshots[0].content).toBe("Checking status. ");
          expect(snapshots[0].hasToolCalls).toBe(false);
          expect(snapshots[0].isComplete).toBe(false);

          // Snapshot 2: accumulated content
          expect(snapshots[1].content).toBe("Checking status. Please wait.");
          expect(snapshots[1].hasToolCalls).toBe(false);
          expect(snapshots[1].isComplete).toBe(false);

          // Snapshot 3: content and tool calls
          expect(snapshots[2].content).toBe("Checking status. Please wait.");
          expect(snapshots[2].hasToolCalls).toBe(true);
          expect(snapshots[2].toolCalls[0].function.name).toBe("status_check");
          expect(snapshots[2].isComplete).toBe(false);

          // Snapshot 4: finished with tool_calls
          expect(snapshots[3].content).toBe("Checking status. Please wait.");
          expect(snapshots[3].hasToolCalls).toBe(true);
          expect(snapshots[3].isComplete).toBe(true);
          expect(snapshots[3].finishReason).toBe("tool_calls");
          expect(snapshots[3].rawMessage).toEqual({
            role: "assistant",
            content: "Checking status. Please wait.",
            tool_calls: [
              {
                id: "call_check",
                index: 0,
                type: "function",
                function: { name: "status_check", arguments: '{"all": true}' },
              },
            ],
          });
          done();
        },
      });
  });
});

describe("parseToolCallArguments", () => {
  it("parses complete JSON successfully", () => {
    const raw = '{"location": "San Francisco", "temp": 72, "active": true}';
    const result = parseToolCallArguments<{
      location: string;
      temp: number;
      active: boolean;
    }>(raw);

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.isPartial).toBe(false);
      expect(result.data).toEqual({
        location: "San Francisco",
        temp: 72,
        active: true,
      });
    }
  });

  it("parses from ToolCall object input", () => {
    const tc = {
      id: "call_1",
      index: 0,
      type: "function" as const,
      function: {
        name: "test",
        arguments: '{"foo": "bar"}',
      },
    };
    const result = parseToolCallArguments<{ foo: string }>(tc);

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.isPartial).toBe(false);
      expect(result.data.foo).toBe("bar");
    }
  });

  it("repairs and parses streaming partial JSON (unclosed string and braces)", () => {
    const partial = '{"location": "San Fran';
    const result = parseToolCallArguments<{ location: string }>(partial);

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.isPartial).toBe(true);
      expect(result.data.location).toBe("San Fran");
    }
  });

  it("repairs streaming partial JSON with dangling colon or comma", () => {
    const withColon = '{"city": "Paris", "details":';
    const resColon = parseToolCallArguments<{ city: string; details: any }>(
      withColon,
    );
    expect(resColon.success).toBe(true);
    if (resColon.success) {
      expect(resColon.isPartial).toBe(true);
      expect(resColon.data.city).toBe("Paris");
      expect(resColon.data.details).toBeNull();
    }

    const withComma = '{"tags": ["dev", "prod"],';
    const resComma = parseToolCallArguments<{ tags: string[] }>(withComma);
    expect(resComma.success).toBe(true);
    if (resComma.success) {
      expect(resComma.isPartial).toBe(true);
      expect(resComma.data.tags).toEqual(["dev", "prod"]);
    }
  });

  it("repairs empty streaming string to empty object", () => {
    const result = parseToolCallArguments("");
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.isPartial).toBe(true);
      expect(result.data).toEqual({});
    }
  });

  it("fails safely on truly malformed JSON without false positives", () => {
    const malformedCases = [
      "{not valid json}",
      "just a random string",
      '{"key": : 123}',
      "{invalid",
    ];

    for (const raw of malformedCases) {
      const result = parseToolCallArguments(raw);
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toBeInstanceOf(Error);
        expect(result.errorMessage).toBeDefined();
        expect(result.raw).toBe(raw);
      }
    }
  });
});
