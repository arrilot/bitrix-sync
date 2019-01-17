<?php

namespace Arrilot\BitrixSync\Telegram;

use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Handler\MissingExtensionException;
use Monolog\Handler\Curl;
use Monolog\Logger;
use RuntimeException;

class TelegramHandler extends AbstractProcessingHandler
{
    /**
     * @var int|string
     */
    private $token;

    /**
     * @var int|string
     */
    private $chatId;
    
    /**
     * @var string|null
     */
    private $proxy;

    /**
     * @param string $token Telegram API token
     * @param $chatId
     * @param int|string $level The minimum logging level at which this handler will be triggered
     * @param bool $bubble Whether the messages that are handled can bubble up the stack or not
     *
     * @throws MissingExtensionException
     */
    public function __construct(
        $token,
        $chatId,
        $level = Logger::ALERT,
        $bubble = true
    ) {
        if (!extension_loaded('curl')) {
            throw new MissingExtensionException('Curl PHP extension is required to use the TelegramHandler');
        }

        $this->token = $token;
        $this->chatId = $chatId;

        parent::__construct($level, $bubble);
    }

    /**
     * Builds the body of API call.
     *
     * @param array $record
     *
     * @return string
     */
    protected function buildContent(array $record)
    {
        $content = [
            'chat_id' => $this->chatId,
            'text' => $record['formatted'],
        ];

        if ($this->formatter instanceof TelegramFormatter) {
            $content['parse_mode'] = 'HTML';
        }

        return json_encode($content);
    }

    /**
     * @param $proxy
     * @return mixed
     */
    public function setProxy($proxy)
    {
        return $this->proxy = $proxy;
    }

    /**
     * {@inheritdoc}
     *
     * @param array $record
     */
    protected function write(array $record)
    {
        $content = $this->buildContent($record);
        $host = $this->proxy ? $this->proxy : 'https://api.telegram.org';

        $ch = curl_init();

        $headers = ['Content-Type: application/json'];
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ch, CURLOPT_URL, sprintf('%s/bot%s/sendMessage', $host, $this->token));
        curl_setopt($ch, CURLOPT_TIMEOUT_MS, 10000);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $content);

        try {
            Curl\Util::execute($ch);
        } catch (RuntimeException $e) {
            AddMessage2Log($e->getMessage(), "bitrix-sync");
        }
    }
}
