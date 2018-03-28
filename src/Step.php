<?php

namespace Arrilot\BitrixSync;

use Monolog\Logger;

abstract class Step
{
    /**
     * Название шага для отображения в логах.
     * Если не указано то будет использовано FQCN.
     *
     * @var string
     */
    public $name = '';

    /**
     * Массив данных через которых можно передавать ту или иную информацию между шагами.
     *
     * @var array
     */
    protected $shared = [];

    /**
     * @var int
     */
    protected $startTime;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * @var bool
     */
    protected $sqlLogsBitrix = false;

    /**
     * @var bool
     */
    protected $sqlLogsIlluminate = false;

    /**
     * @var string
     */
    protected $status;
    
    /**
     * Массив FQCN классов-шагов от которых данный шаг зависит.
     * @var array
     */
    public $dependsOn = [];

    /**
     * Основной метод.
     */
    abstract public function perform();

    /**
     * @return string
     */
    protected function getName()
    {
        return $this->name ? $this->name : get_called_class();
    }

    /**
     * @return string
     */
    public function getStatus()
    {
        return $this->status;
    }
    
    /**
     * @return $this
     */
    protected function logCurrentMemoryUsage()
    {
        $usage = memory_get_usage(true);
        if ($usage < 1024) {
            $this->logger->info('Current memory usage: ' . $usage . ' B');
        } elseif ($usage < 1024 * 1024) {
            $this->logger->info('Current memory usage: ' . $usage / 1024 . ' KB');
        } else {
            $this->logger->info('Current memory usage: ' . $usage / 1024 / 1024 . ' MB');
        }

        return $this;
    }

    /**
     * @return $this
     */
    protected function logPeakMemoryUsage()
    {
        $usage = memory_get_peak_usage(true);
        if ($usage < 1024) {
            $this->logger->info('Peak memory usage: ' . $usage . ' B');
        } elseif ($usage < 1024 * 1024) {
            $this->logger->info('Peak memory usage: ' . $usage / 1024 . ' KB');
        } else {
            $this->logger->info('Peak memory usage: ' . $usage / 1024 / 1024 . ' MB');
        }

        return $this;
    }
    
    /**
     * @return $this
     */
    protected function logSqlQueriesSinceStepStart()
    {
        if ($this->sqlLogsBitrix) {
            $log = \Bitrix\Main\Application::getConnection()->getTracker()->getQueries();
            $count = count($log);
            $totalTime = 0;
            $details = [];
            foreach ($log as $entry) {
                $type = strtok(ltrim($entry->getSql()), ' ');
                $type = strtolower(strtok($type, "\r\n" ));
                if (!isset($details[$type])) {
                    $details[$type] = ['count' => 0, 'total_time' => 0];
                }
                $details[$type]['count']++;
                $details[$type]['total_time'] += $entry->getTime();
                $totalTime += $entry->getTime();
            }
            $this->logger->info(
                sprintf('SQL запросов bitrix с начала шага: %s, время выполнения %s сек.', $count, $totalTime),
                $details
            );
        }

        if ($this->sqlLogsIlluminate) {
            $log = \Illuminate\Database\Capsule\Manager::getQueryLog();
            $count = count($log);
            $totalTime = 0;
            $details = [];
            foreach ($log as $entry) {
                $type = strtok($entry['query'], ' ');
                if (!isset($details[$type])) {
                    $details[$type] = ['count' => 0, 'total_time' => 0];
                }
                $details[$type]['count']++;
                $details[$type]['total_time'] += $entry['time'] / 1000;
                $totalTime += $entry['time'] / 1000;
            }
            $this->logger->info(
                sprintf('SQL запросов illuminate/database с начала шага: %s, время выполнения %s сек.', $count, $totalTime),
                $details
            );
        }

        return $this;
    }

    /**
     * Эти методы позволяют вклиниться в этапы жизненного цикла шага.
     * Выполняются эти методы именно в таком порядке.
     * Сам метод peform выполняетс между onAfterLogStart и onBeforeLogFinish
     */
    public function onBeforeLogStart() { }
    public function onAfterLogStart() { }
    public function onBeforeLogFinish() { }
    public function onAfterLogFinish() { }
    
    /**
     * Завершает шаг как пропущенный.
     * @param string $message
     * @throws StopStepException
     */
    public function stopAsSkipped($message = '')
    {
        $this->status = 'skipped';
        throw new StopStepException($message);
    }

    /**
     * Завершает шаг как успешно завершенный.
     * @param string $message
     * @throws StopStepException
     */
    public function stopAsFinished($message = '')
    {
        $this->status = 'finished';
        throw new StopStepException($message);
    }

    /**
     * Завершает шаг как проваленный.
     * @param string $message
     * @throws StopStepException
     */
    public function stopAsFailed($message = '')
    {
        $this->status = 'failed';
        throw new StopStepException($message);
    }

    /**
     * Завершает всю синхронизацию.
     * @param string $message
     * @throws StopSyncException
     */
    public function stopEverything($message = '')
    {
        $this->status = 'failed';
        throw new StopSyncException($message);
    }

    /**
     * @param Logger $logger
     * @return $this
     */
    public function setLogger(Logger $logger)
    {
        $this->logger = $logger;

        return $this;
    }

    /**
     * @param $data
     * @return $this
     */
    public function setSharedData(&$data)
    {
        $this->shared = &$data;

        return $this;
    }

    /**
     * @return $this
     */
    public function logStart()
    {
        $this->startTime = microtime(true);
        $this->logger->info('==============================================');
        $this->logger->info(sprintf('Шаг "%s" начат', $this->getName()));

        return $this;
    }

    /**
     * @return $this
     */
    public function logFinish()
    {
        $this->logger->info(sprintf('Шаг "%s" завершён', $this->getName()));
        $time = microtime(true) - $this->startTime;
        if ($time > 60) {
            $this->logger->info("Затраченное время: " . $time / 60 ." минут");
        } else {
            $this->logger->info("Затраченное время: " . $time ." секунд");
        }

        $this->logSqlQueriesSinceStepStart();
        $this->flushSqlLogs();
        $this->logCurrentMemoryUsage();
        $this->logPeakMemoryUsage();

        return $this;
    }

    /**
     * Установка параметров для логирования SQL запросов.
     *
     * @param $sqlLogsBitrix
     * @param $sqlLogsIlluminate
     * @return $this
     */
    public function setSqlLoggingParams($sqlLogsBitrix, $sqlLogsIlluminate)
    {
        $this->sqlLogsBitrix = $sqlLogsBitrix;
        $this->sqlLogsIlluminate = $sqlLogsIlluminate;

        return $this;
    }

    /**
     * Обнуление sql-трэкеров после завершения шага
     */
    public function flushSqlLogs()
    {
        if ($this->sqlLogsBitrix) {
            \Bitrix\Main\Application::getConnection()->getTracker()->reset();
        }

        if ($this->sqlLogsIlluminate) {
            \Illuminate\Database\Capsule\Manager::flushQueryLog();
        }
    }
}
