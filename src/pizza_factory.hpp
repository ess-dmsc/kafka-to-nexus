// Compile with
//clang++  factory_class.cc -o factory_class -std=c++1y
// factory pattern for the creation of a vector of different structures

#include <stdexcept>
#include <iostream>
#include <memory>
#include <vector>


class Pizza {
public:
  virtual int getPrice() const = 0;
  virtual std::string getName() const = 0;
  virtual ~Pizza() {};  /* without this, no destructor for derived Pizza's will be called. */
};

class HamAndMushroomPizza : public Pizza {
public:
  virtual int getPrice() const { return 850; };
  virtual std::string getName() const { return "HamAndMushroom"; }
  virtual ~HamAndMushroomPizza() {};
};

class DeluxePizza : public Pizza {
public:
  virtual int getPrice() const { return 1050; };
  virtual std::string getName() const { return "Deluxe"; }
  virtual ~DeluxePizza() {};
};

class HawaiianPizza : public Pizza {
public:
  virtual int getPrice() const { return 1150; };
  virtual std::string getName() const { return "Hawaiian"; }
  virtual ~HawaiianPizza() {};
};

class Pino {
public:
  enum PizzaType {
    HamMushroom,
    Deluxe,
    Hawaiian
  };

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

