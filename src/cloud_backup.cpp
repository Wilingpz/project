#include<thread>
#include"cloud_backup.hpp"

void compress_test(char *argv[])
{
  //argv[1]=源文件名称
  //argv[2]=压缩包名称 
  _cloud_sys::CompressUtil::Compress(argv[1],argv[2]);
  std::string file = argv[2];
  file += ".txt";
//构建解压缩文件的名称
  _cloud_sys::CompressUtil::UnCompress(argv[2],file.c_str());
}
void data_test()
{
  _cloud_sys::DataManager data_manage("./test.txt");
  data_manage.InitLoad();
  data_manage.Insert("c.txt","c.txt.gz");
  std::vector<std::string>list;
  data_manage.GetAllName(&list);
  for(auto &i : list)
  {
    printf("%s\n",i.c_str());
  }
  printf("----------------------\n");
  list.clear();
  data_manage.NonCompressList(&list);
  for(auto &i : list)
  {
    printf("%s\n",i.c_str());
  }
  data_manage.Insert("a.txt","a.txt");
  data_manage.Insert("b.txt","b.txt.gz");
  data_manage.Insert("c.txt","c.txt");
  data_manage.Insert("d.txt","d.txt.gz");
 data_manage.Storage();//持久化存储
}
void m_non_compress()
{
  _cloud_sys::NonHotCompress ncom(BACKUP_DIR,GZFILE_DIR);
  ncom.Start();
  return;
}
void thr_http_server()
{
  _cloud_sys::Server srv;
  srv.Start();
  return;
}

int main(int argc,char *argv[])
{
 // 文件压缩模块测试
   if(boost::filesystem::exists(GZFILE_DIR) == false)
   {
     boost::filesystem::create_directory(GZFILE_DIR);
   }
   if(boost::filesystem::exists(BACKUP_DIR) == false)
     //文件路径不存在则创建
   {
     boost::filesystem::create_directory(BACKUP_DIR);
   }
  std::thread thr_compress(m_non_compress);//启动非热点文件压缩线程回调函数
  std::thread thr_server(thr_http_server);//网络通信服务端模块启动
  thr_compress.join();//等待线程退出
  thr_server.join();
  //_cloud_sys::Server server;
  //server.Start();
  return 0;
}
