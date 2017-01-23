// Compile with
//clang++  factory_class.cc -o factory_class -std=c++1y
// factory pattern for the creation of a vector of different structures

#include <stdexcept>
#include <iostream>
#include <memory>
#include <vector>

static const int Mega=1024*1024;
static int accum;
static int count;

struct Ingredients {
  
  Ingredients(const std::string& n="Tomato") : name(n) { };
  std::function<void(void*,int)> process_data = [](void* data, int size) {
    //    std::cout << std::string((char*)data) << "\t" << size << std::endl;
    accum+=size;
    if(accum > (Mega)) {
      accum -= Mega;
      std::cout << (++count) << " MB\n";
    }
  };
  const std::string& getName() { return name; }
private:
  const std::string name;
};

class Pizza {
public:
  virtual int getPrice() const = 0;
  virtual std::string getName() const = 0;
  virtual ~Pizza() {};  /* without this, no destructor for derived Pizza's will be called. */
  Ingredients s;
};

class HamAndMushroomPizza : public Pizza {
public:
  virtual int getPrice() const { return 850; };
  virtual std::string getName() const { return "HamAndMushroom"; }
  virtual ~HamAndMushroomPizza() {};
  Ingredients s;
};

class DeluxePizza : public Pizza {
public:
  virtual int getPrice() const { return 1050; };
  virtual std::string getName() const { return "Deluxe"; }
  virtual ~DeluxePizza() {};
  Ingredients s;
};

class HawaiianPizza : public Pizza {
public:
  virtual int getPrice() const { return 1150; };
  virtual std::string getName() const { return "Hawaiian"; }
  virtual ~HawaiianPizza() {};
  Ingredients s;
};

class Pino {
  std::vector<std::string> pizza_list;
public:
  enum PizzaType {
    HamMushroom,
    Deluxe,
    Hawaiian
  };

  Pino() { };
  Pino(std::vector<std::string>&& pizzas) : pizza_list(std::move(pizzas)) { };
  
  static std::unique_ptr<Pizza> createPizza(const std::string& pizzaType) {
    if( pizzaType=="HamAndMushroom" )
      return std::make_unique<HamAndMushroomPizza>();
    if( pizzaType=="Deluxe" )
      return std::make_unique<DeluxePizza>();
    if( pizzaType=="Hawaiian" )
      return std::make_unique<HawaiianPizza>();
    throw "invalid pizza type.";
  }
  
  static std::unique_ptr<Pizza> createPizza(PizzaType pizzaType) {
    switch (pizzaType) {
    case HamMushroom: return std::make_unique<HamAndMushroomPizza>();
    case Deluxe:      return std::make_unique<DeluxePizza>();
    case Hawaiian:    return std::make_unique<HawaiianPizza>();
    }
    throw "invalid pizza type.";
  }
};

