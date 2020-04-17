<?php

declare(strict_types=1);

namespace Maxbanton\Cwh;

interface LogStreamNameStrategyInterface
{
    /**
     * Log stream names must be unique within the log group.
     * Log stream names can be between 1 and 512 characters long.
     * The ':' (colon) and '*' (asterisk) characters are not allowed.
     *
     * @return string
     */
    public function generateStreamName(): string;
}
