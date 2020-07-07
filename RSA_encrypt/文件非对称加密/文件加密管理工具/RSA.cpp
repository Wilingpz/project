#include"RSA.h"


RSA::RSA()
{
	getkey();

}
void RSA::encrypt(const char* filename, const char* fileout)
{
	std::ifstream fin(filename, std::ifstream::binary);//文件读取模式为二进制
	std::ofstream fout(fileout, std::ifstream::binary);//头文件<fstream>
	if (!fin.is_open())
	{
		perror("input file open failed!");
		return;
	}
	char *buffer = new char[NUMBER];//从文件每次读取这么多数据
	DataType * bufferout = new DataType[NUMBER];//按块输出
	while (!fin.eof())//不到文件末尾
	{
		fin.read(buffer, NUMBER);
		int count = fin.gcount();//最近一次读到的字节数（有可能比NUMBER小）
		for (int i = 0; i < count; i++)
		{
			bufferout[i] = encrypt((DataType)buffer[i], _key._ekey, _key._nkey);//加密后的数据是Datatype类型（加密后得到的是一个大数）
		}
		fout.write((char*)bufferout, count * sizeof(DataType));//总共要写的字节数（存放次数*类型字节数）
	}
	delete[]bufferout;
	delete[]buffer;
	fin.close();
	fout.close();
}
void RSA::decode(const char* filename, const char* fileout)
{
	std::ifstream fin(filename, std::ifstream::binary);
	std::ofstream fout(fileout, std::ofstream::binary);
	if (!fin.is_open())
	{
		perror("file open failed!");
		return;
	}
	DataType *buffer = new DataType[NUMBER];//加密完的文件是Datatype类型大小的
	char* bufferout = new char[NUMBER];
	while (!fin.eof())
	{
		fin.read((char*)buffer, NUMBER*sizeof(DataType));
		int num = fin.gcount();//加密后的数据是datatype类型，所以解密时接收的也是datatype
		num /= sizeof(DataType);
		for (int i = 0; i < num; i++)
		{
			bufferout[i] = (char)decode(buffer[i], _key._dkey, _key._nkey);
		}
		fout.write(bufferout, num);
	}
	delete[]bufferout;
	delete[]buffer;
	fout.close();
	fin.close();
}
void RSA::getkey()
{
	DataType prime1 = GetPrime();
	std::cout << "prime1: " << prime1 << std::endl;
	DataType prime2 = GetPrime();
	while (prime1 == prime2)
	{
		prime2 = GetPrime();
	}
	std::cout << "prime2: " << prime2 << std::endl;
	_key._nkey = Getnkey(prime1, prime2);
	std::cout << "getnkey finish!" << std::endl;
	std::cout << "nkey: " <<_key._nkey<< std::endl;
	DataType euler = GetEuler(prime1,prime2);
	std::cout << "euler finish!" << std::endl;
	std::cout << "euler: " <<euler<< std::endl;
	_key._ekey = Getekey(euler);
	std::cout << "getekey finish!" << std::endl;
	std::cout << "ekey: " << _key._ekey<<std::endl;
	_key._dkey = Getdkey(_key._ekey,euler);
	std::cout << "getdkey finish!" << std::endl;
	std::cout << "dkey: " <<_key._dkey<<std::endl;
}
DataType RSA::encrypt(DataType k, DataType ekey, DataType nkey)
{
	/*i=0……最后一位    k^ekey %nkey  */
	DataType Ai = k;
	DataType msgE = 1;//存储结果
	while (ekey)//当指数不为0的时候
	{
		if (ekey & 1)//该位二进制为1则进入
		{
			msgE = (msgE * Ai) % nkey;//A0就是k
		}  
			ekey >>= 1;
			Ai = (Ai * Ai) % nkey;//新的Ai相当于旧的Ai的平方模nkey
	}
	return msgE;
	//return (DataType)pow(k, ekey)%nkey ;//求幂运算会溢出，所以加解密结果不对
	//取模要给整数，但是求幂运算结果不确定，强转
}
DataType RSA::decode(DataType k, DataType dkey, DataType nkey)
{
	return encrypt(k, dkey, nkey);
}
DataType RSA::GetPrime()
{
	std::cout << "getPrime()" << std::endl;
	brdm::mt19937 gen(time(NULL));//种子
	brdm::uniform_int_distribution<DataType>dist(0, DataType(1) << 256);//给范围

	//srand(time(NULL));
	DataType prime;
	while (1)
	{
		//prime = rand() % 100 + 2;//产生2到102之间的任意数
		prime = dist(gen);
		//std::cout << "BigInt Random" << prime << std::endl;
		if (is_Prime(prime))
		{
			break;
		}
	}
	std::cout << "get prime finish!" << std::endl;
	return prime;
}
//bool RSA::is_Prime(DataType prime)
//{
//	std::cout << "IsPrime()" << std::endl;
//	if (prime <= 0)
//	{
//		return false;
//	}
//	for (int i = 2; i < sqrt(prime); i++)
//	{
//		if (prime%i == 0)
//		{
//			return false;
//		}
//	}
//	std::cout << "isPrime finish！" << std::endl;
//	return true;
//}
bool RSA::is_Prime(DataType data)
{
	brdm::mt11213b gen(time(NULL));
	if (miller_rabin_test(data, 25, gen))//检测n是否为素数
	{
		if (miller_rabin_test((data - 1) / 2, 25, gen))//检测data-1是否为素数
		{
			return true;
		}
	}
	return false;
}
DataType RSA::Getnkey(DataType prime1, DataType prime2)
{	         
	return  prime1*prime2;
}
DataType RSA::GetEuler(DataType prime1, DataType prime2)
{
	//p1和p2互质则f(mn)=f(m)f(n)
	//若n为质数则f(n)=n-1
	return (prime1 - 1)*(prime2 - 1);
}
DataType RSA::Getekey(DataType euler)
{
	//1<e<f(n)
	//srand(time(NULL));
	brdm::mt19937 gen(time(NULL));
	brdm::uniform_int_distribution<DataType>dist(2,euler);
	DataType ekey;
	while (1)
	{
		//ekey = rand() % euler;//ekey=rand()%euler+2;不让它取0和1
		ekey = dist(gen);
		if (ekey > 1 && Getgcd(euler, ekey)==1)
		{
			return ekey;
		}
	}
	return ekey;
}
DataType RSA:: Getdkey(DataType ekey, DataType euler)
{
	//de%f(n)=1 
	DataType x = 0, y = 0;
	exGcd(ekey, euler, x, y);
	//只在乎x是多少
	//变换，让解密密钥是一个比较小的整数
	return (x%euler + euler) % euler;


	//DataType dkey = euler / ekey;//搜索法
	//while (1)
	//{
	//	if ((dkey*ekey) % euler == 1)
	//	{
	//		return dkey;
	//	}
	//	++dkey;
	//}
}
//辗转相除法(欧几里得算法)
//a%b=c 当c不为0
//a=b   b=c
DataType RSA::Getgcd(DataType data1, DataType data2)
{
	DataType remainder;
	while (remainder = data1%data2)
	{
		data1 = data2;
		data2 = remainder;
	}
	return data2;
}
//key RSA::getallkey()
//{
//	return _key;
//}
DataType RSA::exGcd(DataType a, DataType b, DataType &x, DataType &y)//传引用(输出型参数或可以做左值)
{
	if (b == 0)
	{
		x = 1;
		y = 0;
		return a;
	}
	DataType gcd=exGcd(b, a%b, x, y);
	DataType x1 = x, y1 = y;
	x = y1;
	y = x1 - a / b*y1;
	return gcd;//返回最大公约数，x,y传的是引用，所以可以直接得到x，y的值
}