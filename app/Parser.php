<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // file is 7GB, memory is 1.5GB... can't read in one go
        // $data = file_get_contents($inputPath);
        $fh = \fopen($inputPath, "r");
        $stats = [];
        while ($line = \fgets($fh)) {
            $comma = \strpos($line, ",", 19);
            $path = \substr($line, 0, $comma - 19); // parse path
            $date = \substr($line, $comma+1, 10); // parse date
            $stats[$path][$date] = ($stats[$path][$date] ?? 0) + 1;
        }

        // sort each array item in $stats by date key
        foreach ($stats as $path => $dates) {
            \ksort($dates, SORT_STRING);
            $stats[$path] = $dates;
        }

        \file_put_contents($outputPath, \json_encode($stats, JSON_PRETTY_PRINT));
        \fclose($fh);
    }
}
