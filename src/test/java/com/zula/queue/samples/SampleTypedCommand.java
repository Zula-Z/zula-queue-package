package com.zula.queue.samples;

import com.zula.queue.core.ZulaCommand;
import com.zula.queue.core.ZulaCommandRetry;

@ZulaCommand(commandType = "typed-command")
@ZulaCommandRetry(maxRetries = 5, retryDelayMs = 2500)
public class SampleTypedCommand {
}
