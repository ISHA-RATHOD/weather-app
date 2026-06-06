import { Component, OnInit, OnDestroy } from '@angular/core';
import { HttpClient, HttpClientModule } from '@angular/common/http';
import { CommonModule } from '@angular/common';
import { Subject, Subscription, forkJoin, of, BehaviorSubject } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap, catchError, finalize } from 'rxjs/operators';
import { environment } from '../environments/environment';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, HttpClientModule],
  templateUrl: './app.component.html',
  styles: [`
    .custom-scrollbar::-webkit-scrollbar { width: 6px; }
    .custom-scrollbar::-webkit-scrollbar-track { background: rgba(30, 41, 59, 0.5); }
    .custom-scrollbar::-webkit-scrollbar-thumb { background: #3b82f6; border-radius: 4px; }
  `]
})
export class AppComponent implements OnInit, OnDestroy {
  private searchSubject = new Subject<string>();
  private sub!: Subscription;

  // સિક્યોરિટી અને ગાર્ડ સ્ટેટ
  isLoggedIn = false;
  
  // નેટવર્ક મ્યુટેશન સ્ટેટ્સ
  isLoading$ = new BehaviorSubject<boolean>(false);
  hasError$ = new BehaviorSubject<boolean>(false);

  weatherData: any;
  forecastList: any[] = [];
  avg4DaysTemp: number = 0;
  activeCity: string = 'Bengaluru';

  constructor(private http: HttpClient) {}

  ngOnInit(): void {
    // ઓટોમેટિક બાયપાસ ગાર્ડ અથવા મેન્યુઅલ લોગિન લોજિક
    if (localStorage.getItem('token')) {
      this.isLoggedIn = true;
      this.getUserLocation();
    }
    this.setupSearchStream();
  }

  handleLogin() {
    localStorage.setItem('token', 'auth-success-token');
    this.isLoggedIn = true;
    this.getUserLocation();
  }

  getUserLocation() {
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (pos) => { this.fetchData('', pos.coords.latitude, pos.coords.longitude); },
        () => { this.fetchData(this.activeCity); }
      );
    } else {
      this.fetchData(this.activeCity);
    }
  }

  setupSearchStream() {
    this.sub = this.searchSubject.pipe(
      debounceTime(600),
      distinctUntilChanged(),
      switchMap((city) => {
        if (!city.trim()) return of(null);
        this.activeCity = city;
        this.isLoading$.next(true);
        this.hasError$.next(false);
        return forkJoin({
          current: this.http.get(`${environment.baseUrl}/weather?q=${city}&appid=${environment.apiKey}&units=metric`).pipe(catchError(() => of(null))),
          forecast: this.http.get(`${environment.baseUrl}/forecast?q=${city}&appid=${environment.apiKey}&units=metric`).pipe(catchError(() => of(null)))
        }).pipe(finalize(() => this.isLoading$.next(false)));
      })
    ).subscribe((res: any) => {
      if (res && res.current && res.forecast) {
        this.weatherData = res.current;
        this.processForecast(res.forecast.list);
      } else if (res) {
        this.hasError$.next(true);
      }
    });
  }

  onSearch(event: Event) {
    const input = event.target as HTMLInputElement;
    this.searchSubject.next(input.value);
  }

  fetchData(city: string, lat?: number, lon?: number) {
    this.isLoading$.next(true);
    this.hasError$.next(false);

    const currentUrl = lat && lon 
      ? `${environment.baseUrl}/weather?lat=${lat}&lon=${lon}&appid=${environment.apiKey}&units=metric`
      : `${environment.baseUrl}/weather?q=${city}&appid=${environment.apiKey}&units=metric`;

    const forecastUrl = lat && lon 
      ? `${environment.baseUrl}/forecast?lat=${lat}&lon=${lon}&appid=${environment.apiKey}&units=metric`
      : `${environment.baseUrl}/forecast?q=${city}&appid=${environment.apiKey}&units=metric`;

    forkJoin({
      current: this.http.get(currentUrl),
      forecast: this.http.get(forecastUrl)
    }).pipe(
      finalize(() => this.isLoading$.next(false))
    ).subscribe({
      next: (res: any) => {
        this.weatherData = res.current;
        this.activeCity = res.current.name;
        this.processForecast(res.forecast.list);
      },
      error: () => this.hasError$.next(true)
    });
  }

  processForecast(list: any[]) {
    const uniqueDays: any[] = [];
    const dateTracker = new Set();
    list.forEach((item) => {
      const date = item.dt_txt.split(' ')[0];
      if (!dateTracker.has(date)) {
        dateTracker.add(date);
        uniqueDays.push(item);
      }
    });
    this.forecastList = uniqueDays.slice(0, 7);
    const next4Days = uniqueDays.slice(1, 5);
    if(next4Days.length > 0) {
      const total = next4Days.reduce((sum, item) => sum + item.main.temp, 0);
      this.avg4DaysTemp = total / next4Days.length;
    }
  }

  retry() { this.fetchData(this.activeCity); }
  ngOnDestroy() { if (this.sub) this.sub.unsubscribe(); }
}