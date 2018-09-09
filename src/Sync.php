<?php

namespace Arrilot\BitrixSync;

use Bitrix\Main\Config\Option;
use FilesystemIterator;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\HandlerInterface;
use Monolog\Handler\NativeMailerHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use LogicException;
use RuntimeException;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Bridge\Monolog\Handler\ConsoleHandler;
use Symfony\Bridge\Monolog\Formatter\ConsoleFormatter;

class Sync
{
    protected $name;
    protected $steps = [];

    /**
     * Название окружения (dev/production).
     * Используется в письмах для понимания откуда они пришли.
     *
     * @var string
     */
    protected $env = '';
    
    protected $logDir = '';
    protected $logFile = '';
    protected $sqlLogsBitrix = false;
    protected $sqlLogsIlluminate = false;
    protected $sharedData = [];

    /**
     * @var Logger
     */
    protected $logger;
    
    /**
     * @var LineFormatter
     */
    protected $formatterForLogger;
    
    /**
     * @var OutputInterface
     */
    protected $output = null;

    /**
     * @var string
     */
    protected $lockFile;

    /**
     * Можно ли допускать наложения синхронизаций одну на другую.
     *
     * @var bool
     */
    protected $allowOverlapping = false;

    /**
     * @var array
     */
    protected $emailsForFinalLog = [];
    
    public function __construct($name)
    {
        if (!preg_match('/^[a-zA-Z0-9_-]+$/', $name)) {
           throw new LogicException(sprintf('Недопустимое имя синхронизации "%s", используйте [a-zA-Z0-9_-]+', $name));
        }

        $this->name = $name;
        $this->env = $this->env();

        $this->logDir = $this->logDir($name);
        if (!file_exists($this->logDir)) {
            mkdir($this->logDir, 0755, true);
        }

        $this->logger = new Logger($this->name);
        $this->formatterForLogger = new LineFormatter("[%datetime%] %level_name%: %message% %context%\n");
        $this->logFile = $this->logDir . '/' . date('Y_m_d_H_i_s') . '.log';
        $handler = (new StreamHandler($this->logFile))->setFormatter($this->formatterForLogger);
        $this->logger->pushHandler($handler);

        $this->lockFile = $this->logDir . '/' . $this->name . '_is_in_process.lock';
    }

    /**
     * Запуск синхронизации
     */
    public function perform()
    {
        $this->logger->info('Синхронизация начата');
        $startTime = microtime(true);
        $this->adjustPhpSettings();
        $this->preventOverlapping();
        $this->normalizeSteps();
        $this->validateSteps();

        if ($this->sqlLogsBitrix) {
            $this->doProfileSql();
        }

        foreach ($this->steps as $step) {
            $step
                ->setLogger($this->logger)
                ->setSqlLoggingParams($this->sqlLogsBitrix, $this->sqlLogsIlluminate)
                ->setSharedData($this->sharedData);

            $step->onBeforeLogStart();
            $step->logStart();
            $step->onBeforeLogStart();

            try {
                $step->perform();
            } catch (StopStepException $e) {
                if ($step->getStatus() === 'failed') {
                    $status = 'проваленный';
                } elseif($step->getStatus() === 'skipped') {
                    $status = 'пропущенный';
                } else {
                    $status = 'успешный';
                }
                $trace = $e->getTrace();
                $this->logger->info('Шаг завершён как '. $status . '.', [
                    'message' => $e->getMessage(),
                    'line' => $trace[0]['line'],
                ]);
    
            } catch (StopSyncException $e) {
                $trace = $e->getTrace();
                $this->logger->info('Получена команда на завершение синхронизации.', [
                    'message' => $e->getMessage(),
                    'line' => $trace[0]['line'],
                ]);
                $step->onBeforeLogFinish();
                $step->logFinish();
                $step->onAfterLogFinish();
                break;
            }

            $step->onBeforeLogFinish();
            $step->logFinish();
            $step->onAfterLogFinish();

            unset($step);
        }

        $this->logger->info('==============================================');
        $this->logger->info('Синхронизация завершена');
        $time = microtime(true) - $startTime;
        if ($time > 60) {
            $this->logger->info("Затраченное время: " . $time / 60 ." минут");
        } else {
            $this->logger->info("Затраченное время: " . $time ." секунд");
        }

        $this->doEmailFinalLog();
    }

    /**
     * Установка шагов синхронизации.
     *
     * @param array $steps
     * @return $this
     */
    public function setSteps(array $steps)
    {
        $this->steps = $steps;

        return $this;
    }
    
    /**
     * Перезапись названия окружения.
     *
     * @param $env
     * @return $this
     */
    public function setEnv($env)
    {
        $this->env = $env;

        return $this;
    }

    /**
     * Установка значения по-умолчанию для массива передаваемого из шага в шаг.
     *
     * @param array $data
     * @return $this
     */
    public function setSharedData(array $data)
    {
        $this->sharedData = $data;

        return $this;
    }

    /**
     * Включает дублирование логов в вывод symfony/console.
     * Требуется дополнительный пакет `symfony/monolog-bridge`
     *
     * @param OutputInterface $output
     * @return $this
     */
    public function sendOutputToSymfonyConsoleOutput(OutputInterface $output)
    {
        if (!class_exists('\Symfony\Bridge\Monolog\Handler\ConsoleHandler')) {
            throw new LogicException('Необходимо выполнить `composer require symfony/monolog-bridge` для использования данного метода');
        }

        $verbosityLevelMap = array(
            OutputInterface::VERBOSITY_QUIET => Logger::ERROR,
            OutputInterface::VERBOSITY_NORMAL => Logger::INFO,
            OutputInterface::VERBOSITY_VERBOSE => Logger::INFO,
            OutputInterface::VERBOSITY_VERY_VERBOSE => Logger::INFO,
            OutputInterface::VERBOSITY_DEBUG => Logger::DEBUG,
        );

        $formatter = new ConsoleFormatter(['format' => "%datetime% %start_tag%%level_name%%end_tag% %message%%context%\n"]);
        $handler = (new ConsoleHandler($output, true, $verbosityLevelMap))->setFormatter($formatter);
        $this->logger->pushHandler($handler);

        return $this;
    }

    /**
     * Включает дублирование логов в echo
     *
     * @return $this
     */
    public function sendOutputToEcho()
    {
        $handler = (new StreamHandler('php://stdout'))->setFormatter($this->formatterForLogger);
        $this->logger->pushHandler($handler);
        
        return $this;
    }

    /**
     * Getter for logger
     */
    public function getLogger()
    {
        return $this->logger;
    }
    
    /**
     * @param HandlerInterface $handler
     * @return Sync
     */
    public function pushLogHandler(HandlerInterface $handler)
    {
        $this->logger->pushHandler($handler);

        return $this;
    }
    
    
    /**
     * Выключает защиту от наложения синхронизаций друг на друга.
     *
     * @return $this
     */
    public function allowOverlapping()
    {
        $this->allowOverlapping = true;
        
        return $this;
    }
    
    /**
     * @return $this
     */
    public function profileSql()
    {
        $this->sqlLogsBitrix = true;
        
        return $this;
    }
    
    /**
     * Посылать ошибки на email. По-умолчанию посылает только критические
     *
     * @param array|string $emails
     * @param int $level
     * @return $this
     */
    public function emailErrorsTo($emails, $level = Logger::ALERT)
    {
        $title = sprintf('%s, %s: ошибка синхронизации "%s"', $this->siteName(), $this->env, $this->name);
        $handler = (new NativeMailerHandler($emails, $title, $this->emailFrom(), $level))->setFormatter($this->formatterForLogger);
        $this->logger->pushHandler($handler);
        
        return $this;
    }

    /**
     * Послать окончательный лог синхронизации после её окончания на указанный email/email-ы.
     *
     * @param array|string $emails
     * @return $this
     */
    public function emailFinalLogTo($emails)
    {
        $this->emailsForFinalLog = is_array($emails) ? $emails : (array) $emails;
        
        return $this;
    }
    
    /**
     * Удаляет все логи старше чем $days дней.
     *
     * @param $days
     * @return $this
     */
    public function cleanOldLogs($days = 30)
    {
        $fileSystemIterator = new FilesystemIterator($this->logDir);
        $now = time();
        foreach ($fileSystemIterator as $file) {
            if ($now - $file->getCTime() >= 60 * 60 * 24 * $days) {
                unlink($this->logDir . '/' . $file->getFilename());
            }
        }

        return $this;
    }
    
    /**
     * Setter for $logdir.
     *
     * @param $dir
     * @return $this
     */
    public function setLogDir($dir)
    {
        $this->logDir = $dir;

        return $this;
    }

    /**
     * Директория куда попадают логи.
     * Важно чтобы в неё не попадало ничего другого кроме файлов создаваемых данным классом
     * иначе при очистке логов они могут быть удалены.
     *
     * @param $name
     * @return string
     */
    protected function logDir($name)
    {
        return logs_path('syncs/'.$name);
    }
    
    /**
     * Название окружения (dev/production).
     * Используется в письмах для понимания откуда они пришли.
     *
     * @return string
     */
    protected function env()
    {
        return class_exists('\Arrilot\DotEnv\DotEnv') ? \Arrilot\DotEnv\DotEnv::get('APP_ENV', 'production') : '';
    }
    
    /**
     * Email с которого посылаются письма.
     *
     * @return string
     */
    protected function emailFrom()
    {
        $emailFrom =  Option::get('main', 'email_from');
        return $emailFrom ? $emailFrom : 'mail@greensight.ru';
    }
    
    /**
     * Имя сайта которое будет отображено в письмах
     *
     * @return string
     */
    protected function siteName()
    {
        return Option::get('main', 'site_name');
    }
    
    /**
     * Метод для донастройки php/приложения перед началом синхронизации.
     */
    protected function adjustPhpSettings()
    {
        $connection = \Bitrix\Main\Application::getConnection();
        
        // выставим максимальный таймаут (8 часов) чтобы mysql не отваливался по нему в случае,
        // если есть какой-то долгий шаг без запросов в базу.
        $connection->query('SET wait_timeout=28800');
        if ($this->checkIlluminate()) {
            \Illuminate\Database\Capsule\Manager::select('SET wait_timeout=28800');
        }
    }

    /**
     * Непосредственно включает профилирование SQL-запросов.
     * @return $this
     */
    protected function doProfileSql()
    {
        global $DB;
        $connection = \Bitrix\Main\Application::getConnection();
        $connection->setTracker(null);
        $connection->startTracker(true);
        $DB->ShowSqlStat = true;
        $DB->sqlTracker = $connection->getTracker();
        
        if ($this->checkIlluminate()) {
            \Illuminate\Database\Capsule\Manager::enableQueryLog();
            $this->sqlLogsIlluminate = true;
        }

        return $this;
    }

    /**
     * @return bool
     */
    protected function checkIlluminate()
    {
        return class_exists('Illuminate\Database\Capsule\Manager');
    }

    /**
     * Защита от наложения синхронизаций друг на друга.
     */
    protected function preventOverlapping()
    {
        if ($this->allowOverlapping) {
            return;
        }

        // проверяем существование lock-файла
        if (file_exists($this->lockFile)) {
            $error = $this->env . ': файл '.$this->lockFile. ' уже существует. Импорт остановлен.';
            $this->logger->alert($error);
            throw new RuntimeException($error);
        }

        // создаем lock-файл
        $fp = fopen($this->lockFile, "w");
        fclose($fp);

        // удаляем lock-файл при завершении скрипта
        register_shutdown_function(function ($lockFile) {
            unlink($lockFile);
        }, $this->lockFile);

        // или получении сигналов на завершение скрипта
        // работает в php 7.1+
        if (function_exists('pcntl_async_signals')) {
            pcntl_async_signals(true);

            pcntl_signal(SIGINT, function ($error) {
                unlink($this->lockFile);
            });
            pcntl_signal(SIGTERM, function ($error) {
                unlink($this->lockFile);
            });
        }
    }

    /**
     * Нормализация формата шагов.
     */
    protected function normalizeSteps()
    {
        foreach ($this->steps as $i => $step) {
            if (is_string($step)) {
                $step = new $step;
                $this->steps[$i] = $step;
            }
        }
    }

    /**
     * Валидация шагов синхронизации.
     */
    protected function validateSteps()
    {
        $names = [];
        foreach ($this->steps as $step) {
            if (!$step instanceof Step) {
                throw new LogicException(get_class($step) . ' is not an instance of ' . Step::class);
            }
            $names[] = get_class($step);
        }

        foreach ($this->steps as $i => $step) {
            foreach ($step->dependsOn as $depends) {
                if (!in_array($depends, $names)) {
                    throw new LogicException(sprintf('There is no "%s" step in sync but "%s" depends on it', $depends, get_class($step)));
                }

                if (array_search($depends, $names) > $i) {
                    throw new LogicException(sprintf('Step "%s" depends on step "%s" and must be run after it', get_class($step), $depends));
                }
            }
        }
    }

    /**
     * Непосредственно отправка лога с результатами синхронизации.
     */
    protected function doEmailFinalLog()
    {
        if (!$this->emailsForFinalLog) {
            return;
        }

        $headers =
            "From: ".$this->emailFrom().PHP_EOL.
            "Content-Type: text/plain; charset=utf-8".PHP_EOL.
            "Content-Transfer-Encoding: 8bit";

        $subject = sprintf('%s, %s: синхронизация "%s" завершёна', $this->siteName(), $this->env, $this->name);
        $message = file_get_contents($this->logFile);
        if (!$message) {
            $message = 'Не удалось получить файл-лог ' . $this->logFile;
        }

        mail(implode(',', $this->emailsForFinalLog), $subject, $message, $headers);
    }
}
