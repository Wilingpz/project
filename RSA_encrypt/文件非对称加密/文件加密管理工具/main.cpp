//#define  _SCL_SECURE_NO_WARNINGS
#include "RSA.h"
#include"BigInt.h"
#include<boost/multiprecision/cpp_int.hpp>
using namespace std;

//void test()
//{
//	RSA rsa;
//	rsa.getkey();
//	key key = rsa.getallkey();
//	DataType original = 2;
//	DataType encrypt = rsa.encrypt(original, key._ekey, key._nkey);
//	cout << "key:" << key._ekey << ' ' << key._dkey << ' ' << key._nkey << endl;
//	cout << "original:" << original << endl;
//	cout << "encrpt:" << encrypt << endl;
//	cout << "decode:" << rsa.decode(encrypt, key._dkey, key._nkey) << endl;
//	 
//}
void TestFile()
{
 //   ifstream fin("test.txt");
	////文本形式打开，一般按照空格或者换行读取数据
	//char a;
	//int b;
	//double c;
	//fin >> a;
	//fin >> b;
	//fin >> c;
	////cout << "a:" << a << "b:" << b << "c:" << c << endl;

	//ifstream finn("test.txt", ifstream::binary);
	//char buffer[100] = {0};
	//finn.read(buffer, 100);
	//cout << finn.gcount() << endl;
	//cout << buffer << endl;

	//字符流
	struct A
	{
		int a;
		char b;
		double c;
	};
	//字符流
	A temp;
	temp.a = 1;
	temp.b = 'b';
	temp.c = 2.2;
	//ofstream fout("testout.txt");
	//fout << temp.a << endl;
	//fout << temp.c << " " << temp.b << endl;
	
	
	//字节流(文本里的东西是乱码)
	/*ofstream fout("testout2.txt",ofstream::binary);
	fout.write((char *)&temp, sizeof(A));
	fout.close();*/
	ifstream fin("testout2.txt", ifstream::binary);
	A b;
	fin.read((char*)&b, sizeof(A));
	cout << b.a << b.b << b.c << endl;
	
	//二进制没有编码规则，所以占的空间更小一点
}
void testRSA()
{
	RSA rsa;
	rsa.encrypt("test.txt", "test.encrypt.txt");
	rsa.decode("test.encrypt.txt", "test.decode.txt");
}


void testAdd()
{
	cout << "-------------------------BigIntAdd---------------------------------" << endl;
	BigInt bi;
	int a = 1198765342;//int可以表达的最大的数是2147483647
	int b = 93476581;
	cout << a + b << endl;
	//cout << bi.add("1198765342", "93476581") << endl;
	BigInt aa = "198765342";
	BigInt bb = "93476581";
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;
	cout << aa + bb << endl;
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;

}
void testSub()
{
	cout << "-------------------------BigIntsub---------------------------------" << endl;
	BigInt bi;
	int a = 1198765342;//int可以表达的最大的数是2147483647
	int b = 93476581;
	cout << a - b << endl;
	//cout << bi.sub("1198765342", "93476581") << endl;
	BigInt aa = "198765342";
	BigInt bb = "93476581";
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;
	cout << aa - bb << endl;
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;
}
void testmul()
{
	cout << "-------------------------BigIntmul---------------------------------" << endl;
	BigInt bi;
	int a = 11;//int可以表达的最大的数是2147483647
	int b = 2;
	cout << a * b << endl;
	//cout << bi.mul("11", "2") << endl;
	BigInt aa = "9834";
	BigInt bb = "99235";
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;
	cout << aa * bb << endl;
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;
}
void testdev()
{
	cout << "-------------------------BigIntdev---------------------------------" << endl;
	BigInt bb = "9834";
	BigInt aa = "99235";
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;

	cout << aa / bb << endl;
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;


	cout << aa%bb << endl;
	cout << "aa" << aa << endl;
	cout << "bb" << bb << endl;

	
}
void testboostBigInt()
{
	//boost::multiprecision::cpp_int ci;

	//ci = 465823920849375867187364757333328390283926483654734091;
	//错误常量太大，对于int计算机最多只能表示十位，42亿九千
	//char num[100] = "12356438574395320340836573829035367289";
	//ci = num;
	string num = "12356438574395320340836573829035367289";
	//ci = num;
	//这里将单参的构造函数都禁掉了，不支持单参构造函数的隐式类型转换
	boost::multiprecision::cpp_int anum(num);//拷贝构造
	//需要调构造函数
    cout << anum << endl;
	cout << anum + 1 << endl;
	boost::multiprecision::int1024_t cpp1024(num);//表示大数位数是固定的，只有1024位
	boost::multiprecision::int1024_t cpp1024_2 = boost::multiprecision::int1024_t(1) << 1023;
	//先构造了一个匿名大数，初始化为1，将其左移1024位
	for ( boost::multiprecision::int1024_t i = 0; i < cpp1024_2; ++i)
	{

	}
	//对于大数来说执行都是非常耗时的
	//可以通过任务管理器查看各个应用的cup分配情况
	cout << "finish！" << endl;
}
void test()
{
	long s = 546763;
	char k = 0;
	k = s;
	cout << k << endl;

}
void testBoostRandom()
{
	boost::random::mt19937 gen(time(NULL));
	boost::random::uniform_int_distribution<DataType>dist(0, DataType(1)<<1023);
	for (int i = 0; i < 100; ++ i)        //类型         范围
	{
		DataType num = dist(gen);
		cout << num << endl;//里面自己有重载输出运算符
	}
}

int main()
{
	//testAdd();
	//testSub();
	//testmul();
	//testdev();
	//testboostBigInt();
	//test();
	testRSA();
	//testBoostRandom();
	system("pause");
	return 0;
}
//出现计算前后aa，bb值不相同的这种情况的原因是因为传的是引用
//传引用其实传的是这两个值本身，在运算过程中有修改这两个数的操作