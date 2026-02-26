<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fh = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($fh, 1 << 24);
        $stats = [];
        $remainder = '';

        while (true) {
            $chunk = \fread($fh, 1 << 24);

            if ($chunk === '' || $chunk === false) {
                break;
            }

            if ($remainder !== '') {
                $chunk = $remainder . $chunk;
                $remainder = '';
            }

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) {
                $remainder = $chunk;
                continue;
            }

            $remainder = \substr($chunk, $lastNl + 1);
            foreach (\explode("\n", \substr($chunk, 0, $lastNl)) as $line) {
                // Domain "https://stitcher.io" = 19 chars (fixed)
                // Datetime e.g. "2022-09-10T13:55:25+00:00" = 25 chars (fixed)
                // Line format: <domain><path>,<datetime>
                // strlen is O(1) in PHP — avoids strpos scanning for the comma
                $len = \strlen($line);
                $path = \substr($line, 19, $len - 45); // 45 = 19 domain + 1 comma + 25 datetime
                $date = \substr($line, $len - 25, 10);
                $byPath = &$stats[$path];
                $byPath[$date] = ($byPath[$date] ?? 0) + 1;
            }
        }

        if ($remainder !== '') {
            $len = \strlen($remainder);
            $path = \substr($remainder, 19, $len - 45);
            $date = \substr($remainder, $len - 25, 10);
            $byPath = &$stats[$path];
            $byPath[$date] = ($byPath[$date] ?? 0) + 1;
        }

        unset($byPath);
        \fclose($fh);

        foreach ($stats as &$dates) {
            \ksort($dates, SORT_STRING);
        }
        unset($dates);

        \file_put_contents($outputPath, \json_encode($stats, JSON_PRETTY_PRINT));
    }
}
