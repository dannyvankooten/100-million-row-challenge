<?php

// JIT is disabled for the challenge, so probably faster without strict types?
// declare(strict_types=1);

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const WORKER_COUNT = 13;
    private const READ_BUF = 4_194_304;
    private const PROBE_SIZE = 2_097_152;

    public function parse($inputPath, $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);

        // ── Enumerate every possible calendar date as a compact 2-byte id ──
        $dateChars  = [];
        $dateLabels = [];
        $totalDates = 0;

        for ($yr = 20; $yr <= 26; $yr++) {
            for ($mo = 1; $mo <= 12; $mo++) {
                $dim = match ($mo) {
                    2       => (($yr + 2000) % 4 === 0) ? 29 : 28,
                    4,6,9,11 => 30,
                    default  => 31,
                };
                $moStr = $mo < 10 ? "0{$mo}" : (string) $mo;
                $pfx   = "{$yr}-{$moStr}-";

                for ($dy = 1; $dy <= $dim; $dy++) {
                    $key = $pfx . ($dy < 10 ? "0{$dy}" : (string) $dy);
                    $dateLabels[$totalDates] = $key;
                    $dateChars[$key] = \chr($totalDates & 0xFF) . \chr($totalDates >> 8);
                    $totalDates++;
                }
            }
        }

        // ── Discover slug→id map by scanning the first ~2 MB ──
        $slugToId   = [];
        $slugList   = [];
        $totalSlugs = 0;

        $probe = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($probe, 0);
        $probeLen = $fileSize > self::PROBE_SIZE ? self::PROBE_SIZE : $fileSize;
        $sample   = \fread($probe, $probeLen);
        \fclose($probe);

        $cur   = 0;
        $bound = \strrpos($sample, "\n");

        while ($cur < $bound) {
            $eol = \strpos($sample, "\n", $cur + 52);
            if ($eol === false) break;
            $slug = \substr($sample, $cur + 25, $eol - $cur - 51);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $totalSlugs;
                $slugList[$totalSlugs] = $slug;
                $totalSlugs++;
            }
            $cur = $eol + 1;
        }
        unset($sample);

        foreach (Visit::all() as $v) {
            $slug = \substr($v->uri, 25);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $totalSlugs;
                $slugList[$totalSlugs] = $slug;
                $totalSlugs++;
            }
        }

        // ── Compute line-aligned chunk boundaries ──
        $edges = [0];
        $bh = \fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            \fseek($bh, (int) ($fileSize * $i / self::WORKER_COUNT));
            \fgets($bh);
            $edges[] = \ftell($bh);
        }
        \fclose($bh);
        $edges[] = $fileSize;

        // ── Fork workers ──
        $dir   = \is_dir('/dev/shm') ? '/dev/shm' : \sys_get_temp_dir();
        $myPid = \getmypid();
        $kids  = [];

        for ($w = 0; $w < self::WORKER_COUNT - 1; $w++) {
            $path = "{$dir}/rc_{$myPid}_{$w}";
            $pid  = \pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Fork failed');
            }
            if ($pid === 0) {
                $result = $this->crunch(
                    $inputPath, $edges[$w], $edges[$w + 1],
                    $slugToId, $dateChars, $totalSlugs, $totalDates,
                );

                // Chunked pack — avoids splatting ~588K args at once
                $fh = \fopen($path, 'wb');
                $batch = [];
                $bc = 0;
                foreach ($result as $v) {
                    $batch[] = $v;
                    if (++$bc === 8192) {
                        \fwrite($fh, \pack('V*', ...$batch));
                        $batch = [];
                        $bc = 0;
                    }
                }
                if ($batch) {
                    \fwrite($fh, \pack('V*', ...$batch));
                }
                \fclose($fh);
                exit(0);
            }
            $kids[] = [$pid, $path];
        }

        // Parent handles the last chunk directly
        $grid = $this->crunch(
            $inputPath,
            $edges[self::WORKER_COUNT - 1],
            $edges[self::WORKER_COUNT],
            $slugToId, $dateChars, $totalSlugs, $totalDates,
        );

        // ── Merge child results ──
        foreach ($kids as [$cpid, $tmpPath]) {
            \pcntl_waitpid($cpid, $st);
            $blob = \file_get_contents($tmpPath);
            \unlink($tmpPath);
            $vals = \unpack('V*', $blob);
            $i = 0;
            foreach ($vals as $v) {
                $grid[$i++] += $v;
            }
        }

        // ── Stream JSON output ──
        $this->writeOutput($outputPath, $grid, $slugList, $dateLabels, $totalSlugs, $totalDates);
    }

    /**
     * Parse a byte-range of the CSV into a flat counts array.
     */
    private function crunch(
        $file, $from, $to,
        $slugToId, $dateChars,
        $totalSlugs, $totalDates,
    ) {
        $bins = \array_fill(0, $totalSlugs, '');

        $fh = \fopen($file, 'rb');
        \stream_set_read_buffer($fh, 0);
        \fseek($fh, $from);
        $left = $to - $from;

        while ($left > 0) {
            $grab  = $left > self::READ_BUF ? self::READ_BUF : $left;
            $chunk = \fread($fh, $grab);
            $cLen  = \strlen($chunk);
            if ($cLen === 0) break;
            $left -= $cLen;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $overshoot = $cLen - $lastNl - 1;
            if ($overshoot > 0) {
                \fseek($fh, -$overshoot, \SEEK_CUR);
                $left += $overshoot;
            }

            $p = 0;
            $safe = $lastNl - 720;

            while ($p < $safe) {
                $nl = \strpos($chunk, "\n", $p + 52);
                $bins[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[\substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $bins[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[\substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $bins[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[\substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $bins[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[\substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $bins[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[\substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $bins[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[\substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;
            }

            while ($p < $lastNl) {
                $nl = \strpos($chunk, "\n", $p + 52);
                if ($nl === false) break;
                $bins[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[\substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;
            }
        }

        \fclose($fh);

        $grid = \array_fill(0, $totalSlugs * $totalDates, 0);

        for ($s = 0; $s < $totalSlugs; $s++) {
            if ($bins[$s] === '') continue;
            $offset = $s * $totalDates;
            foreach (\unpack('v*', $bins[$s]) as $did) {
                $grid[$offset + $did]++;
            }
        }

        return $grid;
    }

    /**
     * Stream well-formatted JSON with buffered writes.
     * Dates come out sorted because the id space is chronological.
     */
    private function writeOutput(
        $outputPath, $grid, $slugList,
        $dateLabels, $totalSlugs, $totalDates,
    ) {
        $fh = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($fh, 1_048_576);

        $dtPrefixes = [];
        for ($d = 0; $d < $totalDates; $d++) {
            $dtPrefixes[$d] = '        "20' . $dateLabels[$d] . '": ';
        }

        $escapedSlugs = [];
        for ($s = 0; $s < $totalSlugs; $s++) {
            $escapedSlugs[$s] = '"\\/blog\\/'
                . \str_replace('/', '\\/', $slugList[$s])
                . '"';
        }

        $buf   = '{';
        $first = true;

        for ($s = 0; $s < $totalSlugs; $s++) {
            $base  = $s * $totalDates;
            $body  = '';
            $comma = '';

            for ($d = 0; $d < $totalDates; $d++) {
                $n = $grid[$base + $d];
                if ($n === 0) continue;
                $body .= $comma . $dtPrefixes[$d] . $n;
                $comma = ",\n";
            }

            if ($body === '') continue;

            $buf .= ($first ? '' : ',')
                . "\n    " . $escapedSlugs[$s] . ": {\n"
                . $body
                . "\n    }";
            $first = false;

            if (\strlen($buf) > 65536) {
                \fwrite($fh, $buf);
                $buf = '';
            }
        }

        $buf .= "\n}";
        \fwrite($fh, $buf);
        \fclose($fh);
    }
}
