<?php

declare(strict_types=1);

namespace Maxbanton\Cwh;

class DateWithRandomSuffixLogStreamNameStrategy implements LogStreamNameStrategyInterface
{
    public const DEFAULT_PART_SEPERATOR = '/';
    public const DEFAULT_TIMEZONE = 'UTC';

    /**
     * @var \DateTimeZone
     */
    protected $dateTimeZone;

    /**
     * @var string
     */
    protected $partSeperator;

    public function __construct(string $partSeperator = null, \DateTimeZone $dateTimeZone = null)
    {
        $this->partSeperator = $partSeperator ?? self::DEFAULT_PART_SEPERATOR;
        $this->dateTimeZone = $dateTimeZone ?? $this->getDefaultDateTimeZone();
    }

    public function generateStreamName(): string
    {
        return implode($this->partSeperator, [
            $this->getDatePrefix(),
            $this->getRandomSuffix(),
        ]);
    }

    protected function getDatePrefix(): string
    {
        return (new \DateTime('now', $this->dateTimeZone))->format(implode($this->partSeperator, ['Y', 'm', 'd']));
    }

    protected function getRandomSuffix(): string
    {
        return bin2hex(\random_bytes(16));
    }

    protected function getDefaultDateTimeZone(): \DateTimeZone
    {
        return new \DateTimeZone(self::DEFAULT_TIMEZONE);
    }
}
