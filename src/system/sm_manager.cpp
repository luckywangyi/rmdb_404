/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <iostream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string &db_name)
{
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string &db_name)
{
    if (is_dir(db_name))
    {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0)
    { // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0)
    { // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string &db_name)
{
    // 数据库不存在
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    // 数据库已经打开
    if (!db_.name_.empty())
    {
        throw DatabaseExistsError(db_name);
    }
    // 将当前工作目录设置为数据库目录
    if (chdir(db_name.c_str()) < 0)
    {
        throw UnixError();
    }

    // 从 DB_META_NAME 文件中加载数据库元数据
    std::ifstream ifs(DB_META_NAME);
    if (ifs.fail())
    {
        throw UnixError();
    }
    ifs >> db_;

    // 打开数据库中每个表的记录文件并读入
    for (auto &entry : db_.tabs_)
    {
        std::string table_name = entry.first;
        TabMeta &tab_meta = entry.second;
        fhs_[table_name] = rm_manager_->open_file(table_name);

        // 打开表的所有索引文件
        for (auto &index_entry : tab_meta.indexes)
        {
            std::string index_name = index_entry.first;
            IndexMeta &index_meta = index_entry.second;
            ihs_[index_name] = ix_manager_->open_index(table_name, index_meta.cols);
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta()
{
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db()
{
    if (db_.name_.empty())
    {
        return;
    }

    flush_meta();

    // 关闭所有表文件句柄
    for (auto &entry : fhs_)
    {
        rm_manager_->close_file(entry.second.get());
    }

    // 关闭所有索引文件句柄
    for (auto &entry : ihs_)
    {
        ix_manager_->close_index(entry.second.get());
    }

    fhs_.clear();
    ihs_.clear();
    db_.name_.clear();
    db_.tabs_.clear();

    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context* context)
{
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_)
    {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的所有索引
 * @param {string&} table_name 表名称
 * @param {Context*} context
 */
void SmManager::show_indexes(std::string &table_name, Context* context)
{
    TabMeta &tab = db_.get_table(table_name);

    std::vector<std::string> captions = {"Table", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print indexes
    for (auto &index_entry : tab.indexes)
    {
        std::vector<std::string> index_info = {tab.name, index_entry.first};
        printer.print_record(index_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string& tab_name, Context* context)
{
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols)
    {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context)
{
    if (db_.is_table(tab_name))
    {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs)
    {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context)
{
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }

    TabMeta &tab = db_.get_table(tab_name);

    // 先删除该表的所有索引
    std::vector<std::string> index_names;
    for (auto &index_entry : tab.indexes)
    {
        index_names.push_back(index_entry.first);
    }

    for (auto &index_name : index_names)
    {
        auto &index_meta = tab.indexes.at(index_name);
        drop_index(tab_name, index_meta.cols, context);
    }

    // 关闭表文件
    fhs_.erase(tab_name);

    // 删除表文件
    rm_manager_->destroy_file(tab_name);

    // 从数据库元数据中删除表
    db_.tabs_.erase(tab_name);

    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context)
{
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }

    // 获取表元数据
    TabMeta &tab = db_.get_table(tab_name);

    // 收集索引列元数据
    std::vector<ColMeta> cols;
    for (auto &col_name : col_names)
    {
        auto col = tab.get_col(col_name);
        if (col == tab.cols.end())
        {
            throw ColumnNotFoundError(col_name);
        }
        cols.push_back(*col);
    }

    // 检查是否已存在相同的索引
    std::string ix_name = ix_manager_->get_index_name(tab_name, cols);
    if (tab.indexes.find(ix_name) != tab.indexes.end())
    {
        throw IndexExistsError(tab_name, col_names);
    }

    // 创建索引文件
    ix_manager_->create_index(tab_name, cols);
    ihs_[ix_name] = ix_manager_->open_index(tab_name, cols);

    // 向索引中插入现有记录
    auto fh = fhs_[tab_name].get();
    RmScan scan(fh);
    while (!scan.is_end())
    {
        auto rid = scan.rid();
        auto record = fh->get_record(rid, context);

        // 构建索引键
        int col_tot_len = 0;
        for (auto &col : cols) {
            col_tot_len += col.len;
        }
        char *key = new char[col_tot_len];
        int offset = 0;
        for (auto &col : cols)
        {
            memcpy(key + offset, record->data + col.offset, col.len);
            offset += col.len;
        }

        // 插入索引
        ihs_[ix_name]->insert_entry(key, rid, context->txn_);
        delete[] key;
        scan.next();
    }

    // 更新表元数据
    IndexMeta idx_meta;
    idx_meta.tab_name = tab_name;
    idx_meta.cols = cols;
    idx_meta.col_num = cols.size();
    int col_tot_len = 0;
    for (auto &col : cols)
    {
        col_tot_len += col.len;
        auto col_meta = tab.get_col(col.name);
        col_meta->index = true;
    }
    idx_meta.col_tot_len = col_tot_len;
    idx_meta.index_name = ix_name;

    // 添加索引元数据到表中
    tab.indexes[ix_name] = idx_meta;
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context)
{
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }

    TabMeta &tab = db_.get_table(tab_name);

    // 收集索引列元数据
    std::vector<ColMeta> cols;
    for (auto &col_name : col_names)
    {
        auto col = tab.get_col(col_name);
        if (col == tab.cols.end())
        {
            throw ColumnNotFoundError(col_name);
        }
        cols.push_back(*col);
    }

    // 删除索引
    drop_index(tab_name, cols, context);
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context)
{
    auto &table_meta = db_.get_table(tab_name);
    auto ix_name = ix_manager_->get_index_name(tab_name, cols);
    std::vector<std::string> col_names;
    col_names.reserve(cols.size());
    for (auto &col : cols)
    {
        col_names.push_back(col.name);
    }

    if (table_meta.indexes.find(ix_name) == table_meta.indexes.end())
    {
        throw IndexNotFoundError(tab_name, col_names);
    }

    // 如果索引文件不存在，报错
    if (!disk_manager_->is_file(ix_name))
    {
        throw IndexNotFoundError(tab_name, col_names);
    }

    // 关闭索引句柄
    ix_manager_->close_index(ihs_[ix_name].get());
    // 删除索引文件
    ix_manager_->destroy_index(tab_name, cols);
    // 从索引句柄映射中删除
    ihs_.erase(ix_name);
    // 从表元数据中删除索引
    table_meta.indexes.erase(ix_name);

    // 更新表中的列索引标志
    for (auto &col : cols)
    {
        auto col_meta = table_meta.get_col(col.name);
        col_meta->index = false;
    }

    flush_meta();
}