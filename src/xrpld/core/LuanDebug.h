#pragma once

#include <xrpl/basics/Log.h>

#include <iostream>
#include <mutex>
#include <sstream>
#include <utility>

namespace ripple {

inline Logs* gLuanLogs = nullptr;
inline std::mutex gLuanMutex;

template <class... Args>
inline void
luan(Args&&... args)
{
    std::ostringstream oss;
    oss << "[luan] ";
    (oss << ... << std::forward<Args>(args));

    std::lock_guard<std::mutex> lock(gLuanMutex);
    std::cerr << oss.str() << '\n';
}

template <class... Args>
inline void
luan_j(Args&&... args)
{
    std::ostringstream oss;
    oss << "[luan] ";
    (oss << ... << std::forward<Args>(args));

    if (gLuanLogs)
    {
        JLOG(gLuanLogs->journal("Luan").info()) << oss.str();
        return;
    }

    std::lock_guard<std::mutex> lock(gLuanMutex);
    std::cerr << oss.str() << '\n';
}

}  // namespace ripple
