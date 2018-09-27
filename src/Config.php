<?php

namespace Arrilot\BitrixSync;

use Bitrix\Main\Config\Configuration;

class Config
{
    /**
     * @param $key
     * @param null $default
     * @return mixed
     */
    public function get($key, $default = null)
    {
        $value = Configuration::getValue($key);
        
        return is_null($value) ? $default : $value;
    }
}
