0. Set-up tools: tests_init.zsh

- flutter pub global activate coverage
- brew install lcov

1. Run tests and generate coverage: tests_run.zsh

a. Run tests producing coverage file in 'lcov' folder

flutter pub run test --coverage lcov

b. Generate LCOV file

flutter pub global run coverage:format_coverage --lcov --in=lcov/test --out=lcov/lcov.info --packages=.packages --report-on=lib

c. Generate HTML report in 'lcov/html' folder

genhtml lcov/lcov.info -o lcov/html
