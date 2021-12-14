flutter pub run test --coverage lcov
flutter pub global run coverage:format_coverage --lcov --in=lcov/test --out=lcov/lcov.info --packages=.packages --report-on=lib
genhtml lcov/lcov.info -o lcov/html