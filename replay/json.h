
#ifndef MYJSON_H
#define MYJSON_H

#include <xrpl/json/json_errors.h>
#include <xrpl/json/json_reader.h>
#include <xrpl/json/json_value.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>

json::Value const&
get(json::Value const& obj, char const* key)
{
    if (!obj.isObject() || !obj.isMember(key))
    {
        throw std::runtime_error(std::string("missing json field: ") + key);
    }
    return obj[key];
}

template <class T>
T
get(json::Value const& obj, char const* key)
{
    json::Value const& value = get(obj, key);
    if constexpr (std::is_same_v<T, bool>)
    {
        if (!value.isBool())
            throw std::runtime_error(std::string("JSON field must be bool: ") + key);
        return value.asBool();
    }
    else if constexpr (std::is_same_v<T, std::uint32_t>)
    {
        if (value.isUInt())
            return static_cast<std::uint32_t>(value.asUInt());
        if (value.isInt() && value.asInt() >= 0)
            return static_cast<std::uint32_t>(value.asInt());
        else
            throw std::runtime_error(std::string("JSON field must be uint: ") + key);
    }
    else if constexpr (std::is_same_v<T, std::int64_t>)
    {
        if (value.isInt())
            return static_cast<std::int64_t>(value.asInt());
        if (value.isUInt())
            return static_cast<std::int64_t>(value.asUInt());
        else
            throw std::runtime_error(std::string("JSON field must be int: ") + key);
    }
    else if constexpr (std::is_same_v<T, std::string>)
    {
        if (!value.isString())
            throw std::runtime_error(std::string("JSON field must be string: ") + key);
        return value.asString();
    }
    else
    {
        static_assert(!sizeof(T), "unsupported JSON type");
    }
}
#endif
