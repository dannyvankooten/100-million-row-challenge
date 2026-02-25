<?php

namespace App;


final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // file is 7GB, memory is 1.5GB... can't read in one go
        // $data = file_get_contents($inputPath);
        $fh = fopen($inputPath, "r");
        $i = 0;
        $offset = 0;

        $stats = [];
        while ($line = fgets($fh)) {
            $offset = 19; // skip "https://stitcher.io/"
            $comma = strpos($line, ",", $offset);
            $path = substr($line, $offset, $comma - $offset); // parse path
            $date = substr($line, $comma+1, 10); // parse date
            $stats[$path][$date] = ($stats[$path][$date] ?? 0) + 1;
        }

        // sort each array item in $stats by date key
        foreach ($stats as $path => $dates) {
            ksort($dates);
            $stats[$path] = $dates;
        }

        file_put_contents($outputPath, json_encode($stats, JSON_PRETTY_PRINT));
    }
}
