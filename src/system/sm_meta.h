/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <unordered_map>

#include "errors.h"
#include "sm_defs.h"

/* 字段元数据 */
struct ColMeta
{
    std::string tab_name; // 字段所属表名称
    std::string name;     // 字段名称
    ColType type;         // 字段类型
    int len;              // 字段长度
    int offset;           // 字段位于记录中的偏移量
    bool index;           /** unused */

    friend std::ostream &operator<<(std::ostream &os, const ColMeta &col)
    {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
                  << col.index;
    }

    friend std::istream &operator>>(std::istream &is, ColMeta &col)
    {
        return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index;
    }
};

/* 索引元数据 */
struct IndexMeta
{
    std::string tab_name;      // 索引所属表名称
    std::string index_name;    // 索引文件名
    int col_tot_len;           // 索引字段长度总和
    int col_num;               // 索引字段数量
    std::vector<ColMeta> cols; // 索引包含的字段
    std::vector<int> offsets;  // 每列在索引中的偏移量

    IndexMeta() = default;

    IndexMeta(std::string tab_name_, std::string index_name, int col_tot_len_, int col_num_, std::vector<ColMeta> cols_)
        : tab_name(std::move(tab_name_)),
          index_name(std::move(index_name)),
          col_tot_len(col_tot_len_),
          col_num(col_num_),
          cols(std::move(cols_))
    {
        calculate_offsets();
    }

    void calculate_offsets()
    {
        if (!offsets.empty())
        { // 如果已经计算过，就不需要重新计算
            return;
        }
        offsets.reserve(cols.size());
        int offset = 0;
        for (const auto &col : cols)
        {
            offsets.push_back(offset);
            offset += col.len;
        }
    }

    friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index)
    {
        os << index.tab_name << " " << index.index_name << " " << index.col_tot_len << " " << index.col_num;
        for (auto &col : index.cols)
        {
            os << "\n"
               << col;
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, IndexMeta &index)
    {
        is >> index.tab_name >> index.index_name >> index.col_tot_len >> index.col_num;
        for (int i = 0; i < index.col_num; ++i)
        {
            ColMeta col;
            is >> col;
            index.cols.push_back(col);
        }
        return is;
    }
};

namespace std
{
    template <>
    struct hash<std::vector<std::string>>
    {
        std::size_t operator()(const std::vector<std::string> &vec) const
        {
            std::size_t hash = 0;
            std::hash<std::string> hasher;
            for (const auto &str : vec)
            {
                hash ^= hasher(str) + 0x9e3779b9 + (hash << 6) + (hash >> 2); // Improved hash
            }
            return hash;
        }
    };
}

/* 表元数据 */
struct TabMeta
{
    std::string name;                                                          // 表名称
    std::vector<ColMeta> cols;                                                 // 表包含的字段
    std::unordered_map<std::string, IndexMeta> indexes;                        // 表上建立的索引
    std::unordered_map<std::vector<std::string>, std::string> index_names_map; // Cache for index names

    TabMeta() {}

    TabMeta(const TabMeta &other)
    {
        name = other.name;
        for (auto col : other.cols)
            cols.push_back(col);
    }

    /* 判断当前表中是否存在名为col_name的字段 */
    bool is_col(const std::string &col_name) const
    {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col)
                                { return col.name == col_name; });
        return pos != cols.end();
    }

    /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
    bool is_index(const std::vector<std::string> &col_names)
    {
        auto ix_name = get_index_name(col_names);
        return indexes.count(ix_name);
    }

    /* 根据字段名称集合获取索引元数据 */
    IndexMeta &get_index_meta(const std::vector<std::string> &col_names)
    {
        auto it = indexes.find(get_index_name(col_names));
        if (it != indexes.end())
        {
            return it->second;
        }
        throw IndexNotFoundError(name, col_names);
    }

    /* 根据字段名称获取字段元数据 */
    std::vector<ColMeta>::iterator get_col(const std::string &col_name)
    {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col)
                                { return col.name == col_name; });
        if (pos == cols.end())
        {
            throw ColumnNotFoundError(col_name);
        }
        return pos;
    }

    /* 获取索引名称，使用缓存避免重复字符串操作 */
    std::string get_index_name(const std::vector<std::string> &index_cols)
    {
        auto it = index_names_map.find(index_cols);
        if (it == index_names_map.end())
        {
            std::string ix_name(name);
            for (const auto &col : index_cols)
            {
                ix_name.append("_").append(col);
            }
            ix_name.append(".idx");
            it = index_names_map.emplace(index_cols, std::move(ix_name)).first;
        }
        return it->second;
    }

    friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab)
    {
        os << tab.name << '\n'
           << tab.cols.size() << '\n';
        for (auto &col : tab.cols)
        {
            os << col << '\n'; // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes.size() << "\n";
        for (auto &[index_name, index] : tab.indexes)
        {
            os << index_name << '\n';
            os << index << "\n";
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, TabMeta &tab)
    {
        size_t n;
        is >> tab.name >> n;
        for (size_t i = 0; i < n; i++)
        {
            ColMeta col;
            is >> col;
            tab.cols.push_back(col);
        }
        is >> n;

        for (size_t i = 0; i < n; ++i)
        {
            std::string index_name;
            IndexMeta index;
            is >> index_name;
            is >> index;
            tab.indexes.emplace(std::move(index_name), std::move(index));
            index.cols.clear();
        }
        return is;
    }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和 >>
/* 数据库元数据 */
class DbMeta
{
    friend class SmManager;

private:
    std::string name_;                    // 数据库名称
    std::map<std::string, TabMeta> tabs_; // 数据库中包含的表

public:
    // DbMeta(std::string name) : name_(name) {}

    /* 判断数据库中是否存在指定名称的表 */
    bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

    void SetTabMeta(const std::string &tab_name, const TabMeta &meta)
    {
        tabs_[tab_name] = meta;
    }

    /* 获取指定名称表的元数据 */
    TabMeta &get_table(const std::string &tab_name)
    {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end())
        {
            throw TableNotFoundError(tab_name);
        }

        return pos->second;
    }

    // 重载操作符 <<
    friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta)
    {
        os << db_meta.name_ << '\n'
           << db_meta.tabs_.size() << '\n';
        for (auto &entry : db_meta.tabs_)
        {
            os << entry.second << '\n';
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, DbMeta &db_meta)
    {
        size_t n;
        is >> db_meta.name_ >> n;
        for (size_t i = 0; i < n; i++)
        {
            TabMeta tab;
            is >> tab;
            db_meta.tabs_[tab.name] = tab;
        }
        return is;
    }
};
